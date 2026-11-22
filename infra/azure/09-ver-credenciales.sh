#!/usr/bin/env bash
# Muestra las credenciales de las consolas (RabbitMQ y Kafka UI) guardadas en la VM.
# Solo para el dueño de la suscripción. No copiar estas claves al repositorio.
set -euo pipefail
cd "$(dirname "$0")"
source ./variables.sh
az vm run-command invoke -g "$RG" -n "$VM" --command-id RunShellScript --query "value[0].message" -o tsv --scripts '
grep -E "^(RABBITMQ_USER|RABBITMQ_PASSWORD|KAFKA_UI_USER|KAFKA_UI_PASSWORD)=" /opt/pedidos360/.env
'
echo "RabbitMQ: http://$(ip_vm):15672  ·  Kafka UI: http://$(ip_vm):8090"
