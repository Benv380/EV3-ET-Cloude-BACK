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

paso "PostgreSQL $PG (B1ms, se crea en segundo plano: ~5-10 min)"
PG_LOG="$HOME/pg-create.log"
if ! existe az postgres flexible-server show -g "$RG" -n "$PG"; then
  # Clave temporal: el paso 2 la reemplaza por una nueva y la guarda en el .env de la VM.
  PG_TMP="Tmp$(openssl rand -hex 12)Aa1"
  ( az postgres flexible-server create -g "$RG" -n "$PG" -l "$LOC" \
      --tier Burstable --sku-name Standard_B1ms --storage-size 32 --version 16 \
      --admin-user "$PG_ADMIN" --admin-password "$PG_TMP" --public-access None \
      --high-availability Disabled --backup-retention 7 --tags "${TAGS[@]}" --yes -o none \
    && az postgres flexible-server db create -g "$RG" -s "$PG" -d pedidos -o none \
    && echo "PG-OK" ) > "$PG_LOG" 2>&1 &
  PG_PID=$!
fi

paso "VM $VM ($VM_SIZE) con Docker"
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
  az vm create -g "$RG" -n "$VM" -l "$LOC" --size "$VM_SIZE" --image Ubuntu2204 \
    --admin-username azureuser --generate-ssh-keys --public-ip-sku Standard \
    --os-disk-size-gb 64 --storage-sku StandardSSD_LRS --nsg-rule NONE \
    --custom-data "$CLOUD_INIT" --tags "${TAGS[@]}" -o none
fi
# Puertos públicos: 80 gateway (backend que usa API Management) · 8090 Kafka UI · 15672/15673 consolas RabbitMQ.
# El 22 (SSH) queda cerrado: la VM se administra con "az vm run-command".
az vm open-port -g "$RG" -n "$VM" --port 80,8090,15672,15673 --priority 900 -o none
VM_IP="$(ip_vm)"
echo "IP pública de la VM: $VM_IP"

if [[ -n "${PG_PID:-}" ]]; then
  paso "Esperando PostgreSQL"
  wait "$PG_PID" || true
  cat "$PG_LOG"
fi
paso "Firewall de PostgreSQL: solo la IP de la VM"
az postgres flexible-server firewall-rule create -g "$RG" -n "$PG" --rule-name vm-pedidos360 \
  --start-ip-address "$VM_IP" --end-ip-address "$VM_IP" -o none

paso "Resumen"
az resource list -g "$RG" --query "[].{nombre:name, tipo:type}" -o table
echo "VM:        $VM_IP"
echo "Frontend:  $(url_front)"
echo "APIM:      https://${APIM}.azure-api.net  (estado: $(az apim show -g "$RG" -n "$APIM" --query provisioningState -o tsv 2>/dev/null || echo creando))"
echo "Postgres:  ${PG}.postgres.database.azure.com"
