#!/usr/bin/env bash
# Actualiza el backend ya desplegado: git pull en la VM y recompila/reinicia solo los servicios indicados.
#   bash 05-actualizar-backend.sh pedidos-service analitica-service kafka-admin-service
# Sin argumentos recompila los 6 microservicios (de a uno).
set -euo pipefail
cd "$(dirname "$0")"
source ./variables.sh

SERVICIOS="${*:-pedidos-service notificaciones-service documentos-service rabbit-admin-service analitica-service kafka-admin-service}"
echo "=== Actualizando en la VM: $SERVICIOS ==="
az vm run-command invoke -g "$RG" -n "$VM" --command-id RunShellScript --query "value[0].message" -o tsv --scripts "
cd /opt/pedidos360 && git pull -q && git log --oneline -1
nohup sh -c 'for s in $SERVICIOS; do echo \"### build \$s\"; docker compose build \$s || exit 1; done; docker compose up -d $SERVICIOS && echo \"### UPDATE-OK\"' > /var/log/pedidos360-update.log 2>&1 &
echo ACTUALIZACION-INICIADA
"
echo "Avance: bash 08-estado-vm.sh   (log en la VM: /var/log/pedidos360-update.log)"
