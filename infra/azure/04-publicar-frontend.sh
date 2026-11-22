#!/usr/bin/env bash
# Paso 4: compila el frontend Angular con la configuración de la nube y lo publica en el sitio web estático
# del Storage. Ejecutar en Azure Cloud Shell.
#   AWS_API_URL=https://xxxx.execute-api.us-east-1.amazonaws.com bash 04-publicar-frontend.sh   (cuando exista)
set -euo pipefail
cd "$(dirname "$0")"
source ./variables.sh

FRONT="$(url_front)"
GW_AZURE="https://${APIM}.azure-api.net"
LOGOUT="${COGNITO_DOMINIO}/logout?client_id=${COGNITO_CLIENT_ID}&logout_uri=$(jq -rn --arg u "${FRONT}/" '$u|@uri')"

echo "=== Node 22 (Angular 22 lo exige) ==="
export NVM_DIR="$HOME/.nvm"
[[ -s "$NVM_DIR/nvm.sh" ]] || curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash >/dev/null
# shellcheck disable=SC1091
source "$NVM_DIR/nvm.sh"
nvm install 22 >/dev/null && nvm use 22 >/dev/null
node -v

echo "=== Código del frontend ==="
DIR="$HOME/pedidos360-front"
[[ -d "$DIR/.git" ]] && git -C "$DIR" pull -q || git clone -q "$REPO_FRONT" "$DIR"
cd "$DIR"

echo "=== config.json para la nube ==="
jq -n --arg gwAzure "$GW_AZURE" --arg gwAws "$AWS_API_URL" --arg iss "$COGNITO_ISSUER" \
      --arg cid "$COGNITO_CLIENT_ID" --arg logout "$LOGOUT" '
  def prov($id; $nube; $mgr; $url): {id:$id, nube:$nube, idaas:"Amazon Cognito", apiManager:$mgr,
      apiBaseUrl:$url, issuer:$iss, clientId:$cid, scope:"openid email profile", logoutUrl:$logout};
  {authHabilitada:true, apiBaseUrlLocal:"http://localhost:8080",
   proveedores: ([prov("azure";"Azure";"Azure API Management";$gwAzure)]
                 + (if $gwAws == "" then [] else [prov("aws";"AWS";"AWS API Gateway";$gwAws)] end))}' \
  > public/config.json
cat public/config.json

echo "=== Compilando (npm ci + ng build) ==="
npm ci --no-audit --no-fund --loglevel=error
npx ng build --configuration production

echo "=== Publicando en el Storage $ST ==="
az storage blob upload-batch --account-name "$ST" --auth-mode key -d '$web' \
  -s dist/pedidos-frontend/browser --overwrite -o none
echo "Frontend publicado: $FRONT"
curl -s -o /dev/null -w "GET $FRONT -> %{http_code}\n" "$FRONT/"
