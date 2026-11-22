#!/usr/bin/env bash
# Registra la API de Pedidos 360 en AWS API Gateway (HTTP API) con un authorizer JWT de Amazon Cognito.
# Ejecutar en AWS CloudShell (consola del Learner Lab, región us-east-1):
#   curl -fsSL https://raw.githubusercontent.com/Benv380/EV3-ET-Cloude-BACK/main/infra/aws/crear-api-gateway.sh | bash
# Idempotente: si la API ya existe, la reimporta (actualiza rutas, CORS y authorizer).
set -euo pipefail

NOMBRE="pedidos360-ev3-api"
VM_IP="${VM_IP:-52.162.183.96}"                                        # gateway del backend en Azure (puerto 80)
FRONT="${FRONT:-https://stpedidos360ev3f09d30.z15.web.core.windows.net}" # frontend (CORS)
REGION="us-east-1"
POOL="us-east-1_5eJIGWvjD"
CLIENT="5he9h20pmen4d2kflq7fe7pi6u"
SPEC="$(mktemp --suffix=.yaml)"

echo "=== Contrato OpenAPI (52 operaciones) ==="
curl -fsSL https://raw.githubusercontent.com/Benv380/EV3-ET-Cloude-BACK/main/infra/api/pedidos360-openapi-aws.yaml -o "$SPEC"
sed -i -e "s#http://IP_BACKEND:8080#http://${VM_IP}#g" \
       -e "s#cognito-idp.REGION.amazonaws.com/USER_POOL_ID#cognito-idp.${REGION}.amazonaws.com/${POOL}#" \
       -e "s#CLIENT_ID_COGNITO#${CLIENT}#" \
       -e "s#ORIGEN_FRONTEND#${FRONT}#" \
       -e "s#^  title: pedidos360-api#  title: ${NOMBRE}#" "$SPEC"
grep -q "IP_BACKEND\|USER_POOL_ID\|ORIGEN_FRONTEND" "$SPEC" && { echo "Quedaron marcadores sin reemplazar"; exit 1; }

API_ID="$(aws apigatewayv2 get-apis --region "$REGION" --query "Items[?Name=='${NOMBRE}'].ApiId | [0]" --output text)"
if [[ "$API_ID" == "None" || -z "$API_ID" ]]; then
  echo "=== Importando la API $NOMBRE ==="
  API_ID="$(aws apigatewayv2 import-api --region "$REGION" --no-fail-on-warnings --body "file://$SPEC" --query ApiId --output text)"
else
  echo "=== Reimportando la API $NOMBRE ($API_ID) ==="
  aws apigatewayv2 reimport-api --region "$REGION" --api-id "$API_ID" --no-fail-on-warnings --body "file://$SPEC" >/dev/null
fi

echo "=== Etapa \$default con despliegue automático ==="
aws apigatewayv2 create-stage --region "$REGION" --api-id "$API_ID" --stage-name '$default' --auto-deploy >/dev/null 2>&1 || true

URL="$(aws apigatewayv2 get-api --region "$REGION" --api-id "$API_ID" --query ApiEndpoint --output text)"
echo "Rutas: $(aws apigatewayv2 get-routes --region "$REGION" --api-id "$API_ID" --query 'length(Items)' --output text)"
echo "Authorizer: $(aws apigatewayv2 get-authorizers --region "$REGION" --api-id "$API_ID" --query 'Items[0].[Name,AuthorizerType]' --output text)"
sleep 5
echo
echo "API Gateway: $URL"
echo "Prueba sin token (401):    $(curl -s -o /dev/null -w '%{http_code}' "$URL/api/pedidos")"
echo "Prueba token falso (401):  $(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer abc.def.ghi' "$URL/api/pedidos")"
