#!/usr/bin/env bash
# Paso 2: configura y levanta el backend (docker compose: RabbitMQ x2, ZooKeeper x3, Kafka x3, Kafka UI,
# 6 microservicios y gateway) en la VM. Ejecutar en Azure Cloud Shell después de 01-crear-infra.sh.
#
# Claves: se generan UNA vez dentro de la VM (/opt/pedidos360-secretos.env, solo root) y nunca se muestran aquí.
# Para verlas (consolas RabbitMQ y Kafka UI):  bash 09-ver-credenciales.sh
set -euo pipefail
cd "$(dirname "$0")"
source ./variables.sh

FRONT="$(url_front)"
VM_IP="$(ip_vm)"
DB_PASS_NUEVA="Pg$(openssl rand -hex 14)Aa1"

echo "=== Configurando la VM $VM ($VM_IP) ==="
SCRIPT_VM="$(cat <<EOF
set -e
for i in \$(seq 1 60); do [ -f /opt/pedidos360-listo ] && break; sleep 10; done
[ -f /opt/pedidos360-listo ] || { echo "cloud-init aun no termina"; exit 1; }
cd /opt/pedidos360 && git pull -q
if [ ! -f /opt/pedidos360-secretos.env ]; then
  g() { openssl rand -hex 16; }
  {
    echo "RABBITMQ_PASSWORD=\$(g)"
    echo "RABBITMQ_ERLANG_COOKIE=\$(g)\$(g)"
    echo "KAFKA_UI_PASSWORD=\$(g | cut -c1-16)"
    echo "DB_PASSWORD=${DB_PASS_NUEVA}"
  } > /opt/pedidos360-secretos.env
  chmod 600 /opt/pedidos360-secretos.env
  echo "ESTADO=NUEVO"
else
  echo "ESTADO=EXISTE"
fi
cat > /opt/pedidos360/.env <<'ENV'
GATEWAY_PORT=80
KAFKA_UI_PORT=8090
KAFKA_UI_USER=admin
RABBITMQ_USER=pedidos
DB_URL=jdbc:postgresql://${PG}.postgres.database.azure.com:5432/pedidos?sslmode=require
DB_USER=${PG_ADMIN}
SEGURIDAD_HABILITADA=true
COGNITO_ISSUER=${COGNITO_ISSUER}
COGNITO_JWK_SET_URI=
COGNITO_CLIENT_ID=${COGNITO_CLIENT_ID}
ENTRA_ISSUER=
ENTRA_JWK_SET_URI=
ENTRA_CLIENT_ID=
ROL_ADMIN=Admin
CORS_ORIGENES=${FRONT},http://localhost:4200
ENV
cat /opt/pedidos360-secretos.env >> /opt/pedidos360/.env
chmod 600 /opt/pedidos360/.env
cd /opt/pedidos360
# Se compila un microservicio a la vez (con 8 GB de RAM, 6 compilaciones Maven en paralelo no caben).
nohup sh -c 'for s in pedidos-service notificaciones-service documentos-service rabbit-admin-service analitica-service kafka-admin-service; do echo "### build \$s"; docker compose build \$s || exit 1; done; docker compose up -d && echo "### COMPOSE-UP-OK"' > /var/log/pedidos360-up.log 2>&1 &
echo "COMPOSE=INICIADO"
EOF
)"

SALIDA="$(az vm run-command invoke -g "$RG" -n "$VM" --command-id RunShellScript \
  --scripts "$SCRIPT_VM" --query "value[0].message" -o tsv)"
echo "$SALIDA" | grep -E "ESTADO=|COMPOSE=|cloud-init|error" || echo "$SALIDA"

if echo "$SALIDA" | grep -q "ESTADO=NUEVO"; then
  echo "=== Asignando la clave nueva al administrador de PostgreSQL ==="
  az postgres flexible-server update -g "$RG" -n "$PG" --admin-password "$DB_PASS_NUEVA" -o none
fi

echo
echo "Listo. La compilación y el arranque toman ~10-15 min. Para ver el avance:"
echo "  bash 08-estado-vm.sh"
