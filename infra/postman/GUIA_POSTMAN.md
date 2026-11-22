# Prueba con Postman · bloqueo por posible fraude

## 1. Importar
Postman → **Import** → `Pedidos360-EV3.postman_collection.json`.

## 2. Obtener el token (Cognito)
1. Clic en la colección → pestaña **Authorization** (ya viene configurada: OAuth 2.0, Authorization Code + PKCE).
2. Abajo: **Get New Access Token** → se abre el navegador con el login de Cognito → iniciar sesión.
3. **Use Token**. Todas las peticiones (menos las de la carpeta 1 que prueban sin token) lo usan.

## 3. Demostrar que el API Manager valida el JWT
Ejecutar la carpeta **1**: sin token → **401**, token inválido → **401**, token válido → **200**.
Para probar AWS en vez de Azure: cambiar la variable `baseUrl` por el valor de `baseUrlAws`.

## 4. Ráfaga de 1000 pedidos (posible fraude)
1. Clic derecho en la carpeta **2** → **Run folder**.
2. **Iterations: 1000**, **Delay: 0 ms** → **Run**.
3. Resultado esperado:
   - los primeros pedidos responden **201** (creados);
   - al superar **10 pedidos en 60 s**, el grupo Kafka `analitica-alertas` detecta **RAFAGA_DE_PEDIDOS**,
     publica el bloqueo en el topic compactado `pedidos.bloqueos` y `pedidos-service` responde **403**
     "Usuario bloqueado por posible fraude" al resto.
   - Unos pocos pedidos extra pueden alcanzar a pasar mientras el bloqueo viaja por Kafka (milisegundos): es la
     consistencia eventual de una arquitectura orientada a eventos.
4. En la consola de Postman (abajo) se ve el conteo `creados=… bloqueados=…`.

## 5. Ver el bloqueo
- Postman, carpeta **3** → *Bloqueos activos* (muestra regla, motivo y hasta cuándo).
- Frontend → **Streaming** → sección *Bloqueos por posible fraude* y alerta **FRAUDE**.
- Kafka UI → topic `pedidos.bloqueos` → mensaje con clave = usuario.
- *Desbloquear usuario* (o el botón en el frontend) publica `activo=false` y el usuario vuelve a poder comprar.

## Reglas (configurables en analitica-service)
| Regla | Condición | Bloqueo |
|---|---|---|
| RAFAGA_DE_PEDIDOS | más de `FRAUDE_MAX_PEDIDOS` (10) pedidos del mismo usuario en `FRAUDE_VENTANA_SEGUNDOS` (60 s) | 15 min |
| MONTO_EXCESIVO | un pedido ≥ `FRAUDE_MONTO_MAXIMO` ($10.000.000) | 15 min |
