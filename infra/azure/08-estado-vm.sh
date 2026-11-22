#!/usr/bin/env bash
# Estado de los contenedores en la VM (sin SSH, vía run-command).
set -euo pipefail
cd "$(dirname "$0")"
source ./variables.sh
az vm run-command invoke -g "$RG" -n "$VM" --command-id RunShellScript --query "value[0].message" -o tsv --scripts '
cd /opt/pedidos360
echo "--- docker compose ps"
docker compose ps --format "table {{.Service}}\t{{.Status}}" 2>/dev/null || echo "(aun no hay contenedores)"
echo "--- ultimas lineas de la compilacion"
tail -n 6 /var/log/pedidos360-up.log 2>/dev/null
echo "--- memoria"
free -m | head -2
echo "--- gateway"
curl -s -o /dev/null -w "http://localhost/health -> %{http_code}\n" http://localhost/health
'
