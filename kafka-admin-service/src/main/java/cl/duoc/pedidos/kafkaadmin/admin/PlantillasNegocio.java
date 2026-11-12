package cl.duoc.pedidos.kafkaadmin.admin;

import cl.duoc.pedidos.kafkaadmin.admin.dto.PlantillaMensaje;
import cl.duoc.pedidos.streaming.config.StreamingProperties;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Mensajes de NEGOCIO para probar la reacción del sistema en tiempo real.
 * Se publican desde la API (POST /plantillas/{id}/publicar) o se copian en Kafka UI → Produce Message.
 * Cada plantilla genera un eventoId y una fecha nuevos, así se puede publicar varias veces.
 */
public class PlantillasNegocio {

    private final StreamingProperties props;
    private final Clock reloj;

    public PlantillasNegocio(StreamingProperties props, Clock reloj) {
        this.props = props;
        this.reloj = reloj;
    }

    public List<PlantillaMensaje> todas() {
        String eventos = props.topicos().eventos().nombre();
        String estados = props.topicos().estados().nombre();
        long id = 90_000 + ThreadLocalRandom.current().nextLong(9_000);
        String ahora = Instant.now(reloj).truncatedTo(ChronoUnit.MILLIS).toString();
        return List.of(
                new PlantillaMensaje("pedido-normal", "Pedido normal", eventos, String.valueOf(id),
                        evento(id, "PEDIDO_CREADO", "Cliente Demo", "45990", "CREADO", null, "NINGUNA", ahora),
                        "El dashboard suma 1 evento y $45.990 al monto vendido en menos de 1 segundo."),
                new PlantillaMensaje("pedido-alto-monto", "Pedido de alto monto", eventos, String.valueOf(id),
                        evento(id, "PEDIDO_CREADO", "Empresa Grande SpA", "2500000", "CREADO", null, "NINGUNA", ahora),
                        "El grupo analitica-alertas genera la alerta ALTO_MONTO (umbral $1.000.000)."),
                new PlantillaMensaje("pedido-cancelado", "Pedido cancelado", eventos, String.valueOf(id),
                        evento(id, "ESTADO_CAMBIADO", "Cliente Demo", "45990", "CANCELADO", "PAGADO", "NINGUNA", ahora),
                        "Se genera la alerta CANCELACION y el gráfico de tipos suma un ESTADO_CAMBIADO."),
                new PlantillaMensaje("snapshot-estado", "Estado actual (topic compactado)", estados, String.valueOf(id),
                        "{\"pedidoId\":" + id + ",\"version\":1,\"cliente\":\"Cliente Demo\",\"estado\":\"ENVIADO\","
                                + "\"total\":45990,\"actualizadoEn\":\"" + ahora + "\"}",
                        "La vista de estados muestra el pedido como ENVIADO. Si se publica otro con la misma clave,"
                                + " la compactación deja solo el último."),
                new PlantillaMensaje("fallo-transitorio", "Falla temporal (reintentos)", eventos, String.valueOf(id),
                        evento(id, "PEDIDO_CREADO", "Cliente Demo", "45990", "CREADO", null, "ANALITICA", ahora),
                        "analitica-eventos falla 4 veces (1 + 3 reintentos: 1 s, 2 s, 4 s) y el mensaje pasa a pedidos.dlt."
                                + " El grupo analitica-alertas lo procesa normal: el error queda aislado."),
                new PlantillaMensaje("evento-invalido", "Payload inválido (sin reintentos)", eventos, String.valueOf(id),
                        "{\"eventoId\":\"" + UUID.randomUUID() + "\",\"version\":1,\"tipo\":\"PEDIDO_CREADO\","
                                + "\"pedidoId\":-5,\"cliente\":\"\",\"total\":-100,\"estado\":\"CREADO\","
                                + "\"origen\":\"kafka-ui\",\"fecha\":\"" + ahora + "\"}",
                        "La validación lo rechaza (pedidoId, cliente y total inválidos) y va DIRECTO al DLT, sin reintentar."),
                new PlantillaMensaje("json-corrupto", "JSON corrupto", eventos, String.valueOf(id),
                        "{ esto no es un json valido",
                        "El deserializador no puede leerlo; el consumidor NO se cae y el mensaje va al DLT con el motivo."));
    }

    public Optional<PlantillaMensaje> buscar(String id) {
        return todas().stream().filter(p -> p.id().equals(id)).findFirst();
    }

    private static String evento(long pedidoId, String tipo, String cliente, String total, String estado,
                                 String estadoAnterior, String simularFallo, String fecha) {
        return "{\"eventoId\":\"" + UUID.randomUUID() + "\",\"version\":1,\"tipo\":\"" + tipo + "\","
                + "\"pedidoId\":" + pedidoId + ",\"cliente\":\"" + cliente + "\",\"total\":" + total + ","
                + "\"estado\":\"" + estado + "\","
                + "\"estadoAnterior\":" + (estadoAnterior == null ? "null" : "\"" + estadoAnterior + "\"") + ","
                + "\"usuario\":\"kafka-ui\",\"origen\":\"kafka-ui\",\"simularFallo\":\"" + simularFallo + "\","
                + "\"fecha\":\"" + fecha + "\"}";
    }
}
