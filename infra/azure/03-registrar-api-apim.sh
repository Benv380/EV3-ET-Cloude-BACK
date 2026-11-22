#!/usr/bin/env bash
# Paso 3: registra la API en Azure API Management (importa el contrato OpenAPI con las 52 operaciones)
# y aplica la política: CORS para el frontend + validate-jwt (token de acceso de Amazon Cognito).
# Ejecutar en Azure Cloud Shell cuando API Management esté "Succeeded".
set -euo pipefail
cd "$(dirname "$0")"
source ./variables.sh

ESTADO="$(az apim show -g "$RG" -n "$APIM" --query provisioningState -o tsv)"
[[ "$ESTADO" == "Succeeded" ]] || { echo "API Management aún está en estado: $ESTADO. Reintentar en unos minutos."; exit 1; }

VM_IP="$(ip_vm)"
FRONT="$(url_front)"
TMP="$(mktemp -d)"

echo "=== Importando la API (backend http://$VM_IP/api) ==="
sed "s#http://IP_BACKEND:8080/api#http://${VM_IP}/api#" ../api/pedidos360-openapi.yaml > "$TMP/openapi.yaml"
az apim api import -g "$RG" -n "$APIM" --api-id "$API_ID" --path api \
  --display-name "Pedidos 360 API" --specification-format OpenApi --specification-path "$TMP/openapi.yaml" \
  --service-url "http://${VM_IP}/api" --protocols https --subscription-required false -o none
echo "Operaciones registradas: $(az apim api operation list -g "$RG" -n "$APIM" --api-id "$API_ID" --query 'length(@)' -o tsv)"

echo "=== Aplicando la política (CORS + validate-jwt de Cognito) ==="
sed -e "s#ORIGEN_FRONTEND#${FRONT}#" \
    -e "s#COGNITO_ISSUER#${COGNITO_ISSUER}#g" \
    -e "s#COGNITO_CLIENT_ID#${COGNITO_CLIENT_ID}#" \
    ../apim/politica-api-cognito.xml > "$TMP/politica.xml"
jq -n --rawfile v "$TMP/politica.xml" '{properties:{format:"rawxml", value:$v}}' > "$TMP/politica.json"
SUB="$(az account show --query id -o tsv)"
az rest --method put -o none \
  --uri "https://management.azure.com/subscriptions/${SUB}/resourceGroups/${RG}/providers/Microsoft.ApiManagement/service/${APIM}/apis/${API_ID}/policies/policy?api-version=2022-08-01" \
  --body @"$TMP/politica.json"

GW="https://${APIM}.azure-api.net"
echo
echo "API Management: $GW/api"
echo "Prueba sin token (debe ser 401): $(curl -s -o /dev/null -w '%{http_code}' "$GW/api/pedidos")"
echo "Prueba con token falso (401):    $(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer abc.def.ghi' "$GW/api/pedidos")"
