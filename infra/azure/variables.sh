#!/usr/bin/env bash
# Nombres de los recursos de Pedidos 360 EV3 en Azure. Lo cargan los demás scripts (source variables.sh).
# Restricciones de la suscripción "Azure for Students":
#   - todos los recursos deben llevar la etiqueta Environment=Development
#   - regiones permitidas: canadacentral, mexicocentral, brazilsouth, northcentralus, westus3
#   - cuota: 4 vCPU de la familia B (la VM B4ms usa justo 4)

LOC="${LOC:-brazilsouth}"
RG="${RG:-rg-pedidos360-ev3}"
TAGS=(Environment=Development Proyecto=Pedidos360-EV3)

# Sufijo estable (derivado de la suscripción) para nombres que deben ser únicos en todo Azure.
SUF="$(az account show --query id -o tsv | tr -d '-' | cut -c1-6)"

VM="vm-pedidos360-ev3"
VM_SIZE="${VM_SIZE:-Standard_B4ms}"
APIM="apim-pedidos360-ev3-${SUF}"
PG="psql-pedidos360-ev3-${SUF}"
PG_ADMIN="pedidosadmin"
ST="stpedidos360ev3${SUF}"
API_ID="pedidos360-api"

REPO_BACK="https://github.com/Benv380/EV3-ET-Cloude-BACK.git"
REPO_FRONT="https://github.com/Benv380/EV3-ET-Cloude-FRONT.git"

# IDaaS: Amazon Cognito (identificadores públicos, no son secretos)
COGNITO_REGION="us-east-1"
COGNITO_POOL="us-east-1_5eJIGWvjD"
COGNITO_CLIENT_ID="5he9h20pmen4d2kflq7fe7pi6u"
COGNITO_ISSUER="https://cognito-idp.${COGNITO_REGION}.amazonaws.com/${COGNITO_POOL}"
COGNITO_DOMINIO="https://us-east-15ejigwvjd.auth.us-east-1.amazoncognito.com"

# AWS API Gateway (se completa cuando se cree; vacío = el frontend solo ofrece Azure)
AWS_API_URL="${AWS_API_URL:-}"

ip_vm() { az vm show -d -g "$RG" -n "$VM" --query publicIps -o tsv; }
url_front() { az storage account show -g "$RG" -n "$ST" --query primaryEndpoints.web -o tsv | sed 's:/*$::'; }
