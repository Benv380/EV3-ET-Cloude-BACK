#!/usr/bin/env bash
# Paso 1: crea la infraestructura de Pedidos 360 EV3 en Azure. Ejecutar en Azure Cloud Shell (Bash):
#   git clone https://github.com/Benv380/EV3-ET-Cloude-BACK.git && cd EV3-ET-Cloude-BACK/infra/azure
#   bash 01-crear-infra.sh
# Crea: grupo de recursos, API Management (Consumo), Storage con sitio web estático (frontend),
#       VM B4ms con Docker (cloud-init) y puertos abiertos, PostgreSQL Flexible Server (B1ms) + base "pedidos".
# Es idempotente: si un recurso ya existe, lo deja como está.
set -euo pipefail
cd "$(dirname "$0")"
source ./variables.sh

paso() { echo; echo "=== $* ==="; }
existe() { "$@" -o none 2>/dev/null; }

paso "Grupo de recursos $RG ($LOC)"
az group create -n "$RG" -l "$LOC" --tags "${TAGS[@]}" -o none

paso "API Management $APIM (Consumo, se crea en segundo plano: ~5-15 min)"
if ! existe az apim show -g "$RG" -n "$APIM"; then
  EMAIL="$(az ad signed-in-user show --query userPrincipalName -o tsv)"
  az apim create -g "$RG" -n "$APIM" -l "$LOC" --sku-name Consumption --sku-capacity 0 \
    --publisher-name "Pedidos 360" --publisher-email "$EMAIL" --tags "${TAGS[@]}" --no-wait -o none
fi

paso "Storage $ST (sitio web estático para el frontend)"
if ! existe az storage account show -g "$RG" -n "$ST"; then
  az storage account create -g "$RG" -n "$ST" -l "$LOC" --sku Standard_LRS --kind StorageV2 \
    --min-tls-version TLS1_2 --allow-blob-public-access false --tags "${TAGS[@]}" -o none
fi
az storage blob service-properties update --account-name "$ST" --auth-mode key \
  --static-website --index-document index.html --404-document index.html -o none

paso "VM $VM con Docker (tamaños a probar: $VM_SIZES)"
CLOUD_INIT="$(mktemp)"
cat > "$CLOUD_INIT" <<'EOF'
#cloud-config
package_update: true
packages: [git, jq]
runcmd:
  - fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
  - echo '/swapfile none swap sw 0 0' >> /etc/fstab
  - curl -fsSL https://get.docker.com | sh
  - usermod -aG docker azureuser
  - git clone https://github.com/Benv380/EV3-ET-Cloude-BACK.git /opt/pedidos360
  - touch /opt/pedidos360-listo
EOF
if ! existe az vm show -g "$RG" -n "$VM"; then
  # La B4ms puede no tener capacidad en una región (SkuNotAvailable): se prueba en las regiones permitidas.
  CREADA=""
  for TAM in ${VM_SIZES}; do
    for REGION_VM in ${VM_LOCS}; do
      echo "Intentando crear la VM $TAM en $REGION_VM..."
      if az vm create -g "$RG" -n "$VM" -l "$REGION_VM" --size "$TAM" --image Ubuntu2204 \
          --admin-username azureuser --generate-ssh-keys --public-ip-sku Standard \
          --os-disk-size-gb 64 --storage-sku StandardSSD_LRS --nsg-rule NONE \
          --custom-data "$CLOUD_INIT" --tags "${TAGS[@]}" -o none 2> "$HOME/vm-error.log"; then
        CREADA="$TAM en $REGION_VM"; break 2
      fi
      grep -oE "(SkuNotAvailable|QuotaExceeded|RequestDisallowedByPolicy|OperationNotAllowed)[^.]*" "$HOME/vm-error.log" | head -1 || true
    done
  done
  [[ -n "$CREADA" ]] || { echo "No se pudo crear la VM en ninguna región: $VM_LOCS"; cat "$HOME/vm-error.log"; exit 1; }
  echo "VM creada en $CREADA"
fi
# Puertos públicos: 80 gateway (backend que usa API Management) · 8090 Kafka UI · 15672/15673 consolas RabbitMQ.
# El 22 (SSH) queda cerrado: la VM se administra con "az vm run-command".
az vm open-port -g "$RG" -n "$VM" --port 80,8090,15672,15673 --priority 900 -o none
VM_IP="$(ip_vm)"
echo "IP pública de la VM: $VM_IP"

VM_LOC="$(az vm show -g "$RG" -n "$VM" --query location -o tsv)"

paso "PostgreSQL $PG (B1ms) en la región de la VM ($VM_LOC): ~5-10 min"
if ! existe az postgres flexible-server show -g "$RG" -n "$PG"; then
  # Clave temporal: el paso 2 la reemplaza por una nueva y la guarda en el .env de la VM.
  PG_TMP="Tmp$(openssl rand -hex 12)Aa1"
  az postgres flexible-server create -g "$RG" -n "$PG" -l "$VM_LOC"     --tier Burstable --sku-name Standard_B1ms --storage-size 32 --version 16     --admin-user "$PG_ADMIN" --admin-password "$PG_TMP" --public-access None     --backup-retention 7 --tags "${TAGS[@]}" --yes -o none
fi
existe az postgres flexible-server db show -g "$RG" -s "$PG" -n pedidos   || az postgres flexible-server db create -g "$RG" -s "$PG" -n pedidos -o none

paso "Firewall de PostgreSQL: solo la IP de la VM"
# Acceso por red pública habilitado, pero el firewall solo deja entrar a la IP de la VM.
az postgres flexible-server update -g "$RG" -n "$PG" --public-access Enabled -o none
az postgres flexible-server firewall-rule create -g "$RG" -s "$PG" -n vm-pedidos360   --start-ip-address "$VM_IP" --end-ip-address "$VM_IP" -o none

paso "Resumen"
az resource list -g "$RG" --query "[].{nombre:name, tipo:type}" -o table
echo "VM:        $VM_IP"
echo "Frontend:  $(url_front)"
echo "APIM:      https://${APIM}.azure-api.net  (estado: $(az apim show -g "$RG" -n "$APIM" --query provisioningState -o tsv 2>/dev/null || echo creando))"
echo "Postgres:  ${PG}.postgres.database.azure.com"
