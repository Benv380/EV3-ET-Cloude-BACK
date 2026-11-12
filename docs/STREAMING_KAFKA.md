# Streaming con Apache Kafka · Pedidos 360 (EV3)

RabbitMQ y Kafka cumplen roles distintos en el sistema:

| | RabbitMQ (EV2) | Kafka (EV3) |
|---|---|---|
| Para qué | **Tareas**: notificar, generar documentos, auditar | **Streaming**: analítica en tiempo real, alertas, estado actual |
| Mensaje | Se borra al confirmarse (ACK) | Queda en el log según la **retención**; cada grupo lleva su propio offset |
| Varios lectores | Una cola por tarea + exchanges | Varios **consumer groups** leen el mismo topic, cada uno a su ritmo |
| Re-procesar | No (salvo DLQ) | Sí: mover el offset del grupo al inicio |

`pedidos-service` publica cada evento en **ambos**: RabbitMQ (tareas) y Kafka (streaming).

---

## 1. Arquitectura del clúster (`docker-compose.kafka.yml`)

```
                ┌──────────── ZooKeeper (quórum de 3) ────────────┐
                │ zookeeper1        zookeeper2        zookeeper3  │  metadatos: brokers vivos,
                └───────────────────────┬─────────────────────────┘  líder de cada partición, configs
                                        │
        ┌───────────────────────────────┼───────────────────────────────┐
     kafka1 (id 1)                   kafka2 (id 2)                   kafka3 (id 3)
     P0 líder · P1 copia · P2 copia  P0 copia · P1 líder · P2 copia  P0 copia · P1 copia · P2 líder
```

- **3 ZooKeeper**: funcionan por mayoría. Con 3 nodos el clúster sigue operando si cae 1 (2 de 3 es mayoría).
  Con 2 nodos, la caída de 1 detendría todo; por eso se usa un número impar.
- **3 brokers**: permiten `replication.factor=3` (una copia de cada partición en cada broker). Uno de los brokers
  es además el **controlador** (asigna líderes).
- `min.insync.replicas=2` + productor `acks=all`: una escritura se confirma cuando al menos 2 copias la tienen.
  - Cae 1 broker → se sigue leyendo y escribiendo (quedan 2 copias en sincronía), sin perder datos.
  - Caen 2 brokers → se puede leer, pero las escrituras se rechazan (protege contra pérdida de datos).
- `unclean.leader.election.enable=false`: nunca se elige como líder una copia desactualizada.
- `auto.create.topics.enable=false`: los topics se crean explícitamente con su configuración.
- Imágenes Confluent 7.7 (Kafka 3.7): última línea con soporte de ZooKeeper (Kafka 4 solo usa KRaft).

## 2. Topics: 2 topics de negocio × 3 particiones × 3 réplicas

| Topic | Clave | Particiones | Réplicas | Limpieza | Retención | Uso |
|---|---|---|---|---|---|---|
| `pedidos.eventos` | pedidoId | 3 | 3 | `delete` | 7 días o 1 GB/partición | Historia de **todo** lo que pasa (analítica, alertas) |
| `pedidos.estados` | pedidoId | 3 | 3 | `compact` | último valor por pedido | **Estado actual** de cada pedido |
| `pedidos.dlt` | la original | 3 | 3 | `delete` | 14 días | Mensajes con error (Dead Letter Topic) |

**Por qué 3 particiones**: la partición es la unidad de paralelismo. Dentro de un consumer group cada partición la
atiende un solo consumidor, así que 3 particiones permiten 3 consumidores en paralelo (uno por broker como líder).
Más particiones no aportan con el volumen actual y consumen más recursos.

**Por qué 3 réplicas**: tolerancia a la caída de 1 broker sin perder datos ni disponibilidad (con `min.insync.replicas=2`).

**Por qué la clave es el pedidoId**: Kafka garantiza el orden **dentro de una partición**. Con la misma clave, todos
los eventos de un pedido (CREADO → PAGADO → DESPACHADO) van a la misma partición y se procesan en orden.

**Por qué dos políticas de limpieza**:
- `delete` (eventos): se guardan 7 días para re-procesar o recalcular métricas; luego se borran por segmentos
  diarios (`segment.ms=1 día`). `retention.bytes` evita que crezca sin límite si hay un peak.
- `compact` (estados): Kafka conserva solo el **último** snapshot de cada pedido. El topic no crece con cada
  cambio de estado, y cualquier servicio puede reconstruir la vista completa leyéndolo desde el inicio
  (`analitica-estados` lo hace al iniciar). Un mensaje con valor `null` (tombstone) elimina el pedido.

## 3. Diseño de los eventos (DTO)

`EventoPedidoStream` (topic `pedidos.eventos`) — un **hecho** inmutable:

```json
{ "eventoId": "uuid", "version": 1, "tipo": "PEDIDO_CREADO", "pedidoId": 42, "cliente": "Ana",
  "total": 45990, "estado": "CREADO", "estadoAnterior": null, "usuario": "ana@correo.cl",
  "origen": "pedidos-service", "simularFallo": "NINGUNA", "fecha": "2026-11-12T15:30:00Z" }
```

`EstadoPedidoSnapshot` (topic `pedidos.estados`) — el **estado actual**:

```json
{ "pedidoId": 42, "version": 1, "cliente": "Ana", "estado": "PAGADO", "total": 45990,
  "actualizadoEn": "2026-11-12T15:31:00Z" }
```

- `eventoId`: permite detectar duplicados (los consumidores son idempotentes).
- `version`: permite evolucionar el esquema sin romper consumidores.
- **No** lleva el correo del cliente: la analítica no lo necesita (minimización de datos personales).
- **Headers de negocio** en cada mensaje: `tipo-evento`, `estado`, `origen`, `version-esquema`, `evento-id`.
  Se ven en Kafka UI junto al mensaje y permiten filtrar sin abrir el JSON.

## 4. Productor (`pedidos-streaming` → `ProductorEventos`)

| Propiedad | Valor | Para qué |
|---|---|---|
| `bootstrap-servers` | los 3 brokers | basta alcanzar uno para descubrir el resto |
| `key/value-serializer` | String / JSON (ObjectMapper de Spring) | DTO → JSON con fechas ISO |
| `acks` | `all` | confirma cuando `min.insync.replicas` copias tienen el mensaje |
| `enable.idempotence` | `true` | un reintento de red no duplica el mensaje |
| `retries` / `delivery.timeout.ms` | 10 / 30 s | reintentos automáticos ante fallas transitorias |
| `linger.ms` / `batch-size` / `compression-type` | 5 ms / 16 KB / lz4 | agrupar y comprimir: más rendimiento |
| `max.block.ms` | 5 s | si Kafka no está, no bloquea la petición HTTP 60 s |

El DTO se **valida antes de enviar** (Bean Validation). Cada envío registra `[KAFKA-SEND] ... -> topic-partición offset=N`.

## 5. Consumidores y consumer groups (`analitica-service`)

| Listener (`@KafkaListener`) | Consumer group | Topic | Concurrencia | Lógica de negocio |
|---|---|---|---|---|
| `analitica-eventos` | `analitica-eventos` | pedidos.eventos | 3 | `MetricasService`: métricas en tiempo real |
| `analitica-alertas` | `analitica-alertas` | pedidos.eventos | 1 | `AlertasService`: reglas ALTO_MONTO y CANCELACION |
| `analitica-estados` | `analitica-estados` | pedidos.estados | 3 | `ProyeccionEstadosService`: vista del estado actual |
| `monitor-dlt` | `monitor-dlt` | pedidos.dlt | 1 | `MonitorDltService`: mensajes con error y su motivo |

- **Balanceo de carga**: `analitica-eventos` tiene 3 consumidores; Kafka asigna 1 partición a cada uno.
  El dashboard muestra cuántos eventos procesó cada consumidor y cada partición.
- **Publicar/suscribir**: `analitica-alertas` es **otro grupo** sobre el mismo topic → recibe **todos** los eventos,
  con sus propios offsets. Un grupo lento o caído no afecta al otro.
- **Resiliencia**: si un consumidor cae, tras `session.timeout.ms=15 s` sus particiones se reasignan a los otros
  (rebalanceo incremental `CooperativeStickyAssignor`: solo se mueven las particiones necesarias).
- **Commit del offset**: `enable-auto-commit=false`, `ack-mode=record` → el offset se confirma después de procesar
  cada registro con éxito. Si el servicio cae a mitad, el mensaje se vuelve a entregar; el `eventoId` evita contarlo dos veces.
- **Lógica separada**: el listener solo valida y delega; los servicios de dominio no conocen Kafka.

## 6. Manejo de errores

```
error recuperable (ErrorTransitorioException)  →  reintento 1 s → 2 s → 4 s  →  pedidos.dlt
payload inválido (EventoInvalidoException)     →  sin reintentos              →  pedidos.dlt
JSON corrupto (ErrorHandlingDeserializer)      →  sin reintentos              →  pedidos.dlt
```

- `DefaultErrorHandler` + `ExponentialBackOffWithMaxRetries(3)` + `DeadLetterPublishingRecoverer`.
- El DLT recibe el mensaje original + headers `kafka_dlt-original-topic/partition/offset`,
  `kafka_dlt-original-consumer-group`, `kafka_dlt-exception-fqcn/message`.
- Logs: `[KAFKA-ERROR] intento N`, `[KAFKA-DLT] topic-partición@offset -> pedidos.dlt motivo=...`.
- El consumidor **no se detiene**: después de enviar al DLT sigue con el siguiente mensaje.
- Para la demo: crear un pedido con *Simular fallo = Analítica (Kafka)*. El grupo `analitica-eventos` falla y
  manda el evento al DLT; el grupo `analitica-alertas` lo procesa normal (el error queda aislado).

## 7. Administración (`kafka-admin-service`, `/api/admin/kafka`)

| Operación | Endpoint |
|---|---|
| Brokers y controlador | `GET /cluster` |
| Salud con impacto (brokers caídos, sub-replicación, particiones sin líder, lag) | `GET /salud` |
| Listar / detalle de topics (líder, réplicas, ISR, offsets, configs) | `GET /topics`, `GET /topics/{t}` |
| Crear topic (particiones, réplicas, `cleanup.policy`, `retention.ms`, `min.insync.replicas`) | `POST /topics` |
| Modificar / restablecer configuración | `PATCH /topics/{t}/configs` |
| Aumentar particiones | `POST /topics/{t}/particiones` |
| Eliminar topic (los del sistema están protegidos) | `DELETE /topics/{t}` |
| Consumer groups: estado, miembros, particiones asignadas, offsets y **lag** | `GET /grupos`, `GET /grupos/{g}` |
| Mover offsets al inicio (reprocesar) o al final (saltar) | `POST /grupos/{g}/offsets` |
| Eliminar grupo vacío | `DELETE /grupos/{g}` |
| Mensajes de negocio de prueba | `GET /plantillas`, `POST /plantillas/{id}/publicar` |

Validaciones antes de llegar al broker (`ValidadorTopicos`): nombre válido y no reservado (`__`), réplicas ≤ brokers
activos, `min.insync.replicas` ≤ réplicas, retención ≥ 1 minuto o -1, `cleanup.policy` válida, solo se permite
**aumentar** particiones, y solo un conjunto de configuraciones seguras.

## 8. Monitoreo: offsets, lag y distribución

- **Offset final** (log-end-offset): posición del próximo mensaje que se escribirá en la partición.
- **Offset confirmado**: hasta dónde procesó el grupo.
- **Lag** = offset final − offset confirmado = mensajes pendientes. Si crece y no baja, el consumo va más lento
  que la producción → agregar consumidores (hasta 1 por partición) o particiones.
- Demo del lag: en *Streaming* pausar `analitica-eventos`, crear 3 pedidos → el lag del grupo sube a 3 (pestaña
  Kafka o Kafka UI → Consumers). Reanudar → baja a 0. El grupo `analitica-alertas` nunca se atrasó.

## 9. Kafka UI (`docker-compose.kafka-ui.yml`, puerto 8090)

Qué mostrar: **Brokers** (3, controlador), **Topics** (`pedidos.eventos` y `pedidos.estados` con 3 particiones,
3 réplicas, ISR completo, configuración de retención/limpieza), **Messages** (clave, partición, offset, headers de
negocio, JSON), **Consumers** (grupos, miembros y lag por partición).

Lógica de negocio visible desde Kafka UI:
- **Enmascaramiento**: el campo `usuario` se muestra enmascarado en los topics `pedidos.*` (dato personal).
- **Headers de negocio** en cada mensaje (`tipo-evento`, `estado`, `origen`, `version-esquema`, `evento-id`).
- **Produce Message**: se pueden publicar mensajes de negocio a mano y el sistema reacciona en tiempo real.
  Las plantillas (`GET /api/admin/kafka/plantillas` o pestaña Kafka → *Probar reacciones*) son:

| Plantilla | Topic | Qué pasa |
|---|---|---|
| Pedido normal | pedidos.eventos | Suma 1 evento y su monto en el dashboard |
| Pedido de alto monto | pedidos.eventos | Alerta **ALTO_MONTO** (≥ $1.000.000) |
| Pedido cancelado | pedidos.eventos | Alerta **CANCELACION** |
| Estado actual | pedidos.estados | La vista de estados cambia; con la misma clave queda solo el último |
| Falla temporal | pedidos.eventos | 3 reintentos y al DLT; el grupo de alertas lo procesa normal |
| Payload inválido | pedidos.eventos | Validación lo rechaza: directo al DLT, sin reintentos |
| JSON corrupto | pedidos.eventos | El consumidor no se cae; directo al DLT con el motivo |

## 10. Casos de uso de streaming en Pedidos 360

1. **Métricas en vivo**: ventas, ticket promedio, eventos por minuto, latencia.
2. **Alertas en tiempo real**: pedidos de alto monto (posible fraude o cliente VIP) y cancelaciones.
3. **Vista materializada**: estado actual de todos los pedidos sin consultar la base de datos de pedidos.
4. **Re-procesamiento**: mover el offset de un grupo al inicio para recalcular con la historia retenida (7 días).
5. **Nuevos consumidores sin tocar el productor**: un nuevo servicio (ej. facturación) crea su propio grupo y lee
   desde el inicio de la retención.
