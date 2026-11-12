package cl.duoc.pedidos.pedidos.mensajeria;

import cl.duoc.pedidos.common.mensajeria.evento.PedidoEvento;
import cl.duoc.pedidos.pedidos.pedido.EventosPedidoPublisher;
import cl.duoc.pedidos.streaming.config.StreamingProperties;
import cl.duoc.pedidos.streaming.evento.EstadoPedidoSnapshot;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.evento.TipoEventoPedido;
import cl.duoc.pedidos.streaming.produccion.ProductorEventos;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * PRODUCTOR Kafka: convierte el evento de dominio en los DTO de streaming y los publica.
 * <ul>
 *   <li><b>pedidos.eventos</b>: TODOS los eventos (creado, cambio de estado, boleta). Historia para analítica.</li>
 *   <li><b>pedidos.estados</b>: snapshot con el estado actual del pedido (topic compactado).</li>
 * </ul>
 * En ambos la clave es el pedidoId: los eventos de un mismo pedido llegan en orden a la misma partición.
 * <p>
 * El envío es asíncrono y no bloquea la respuesta HTTP. Si Kafka no está disponible se registra el error
 * (el pedido y sus tareas en RabbitMQ siguen funcionando): el streaming es analítico, no transaccional.
 */
@Component
public class KafkaEventosPedidoPublisher implements EventosPedidoPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventosPedidoPublisher.class);
    private static final String ORIGEN = "pedidos-service";

    private final ProductorEventos productor;
    private final StreamingProperties props;

    public KafkaEventosPedidoPublisher(ProductorEventos productor, StreamingProperties props) {
        this.productor = productor;
        this.props = props;
    }

    @Override
    public void publicarPedidoCreado(PedidoEvento evento) {
        publicarEvento(evento);
        publicarEstado(evento);
    }

    @Override
    public void publicarCambioEstado(PedidoEvento evento) {
        publicarEvento(evento);
        publicarEstado(evento);
    }

    @Override
    public void solicitarBoleta(PedidoEvento evento) {
        publicarEvento(evento);
    }

    private void publicarEvento(PedidoEvento e) {
        EventoPedidoStream dto = new EventoPedidoStream(e.eventoId(), EventoPedidoStream.VERSION_ACTUAL,
                TipoEventoPedido.valueOf(e.tipo().name()), e.pedidoId(), e.cliente(), e.total(), e.estado(),
                e.estadoAnterior(), e.usuario(), ORIGEN, e.simularFallo().name(), e.fecha());
        enviar(props.topicos().eventos().nombre(), dto.clave(), dto, headers(dto.tipo().name(), e));
    }

    private void publicarEstado(PedidoEvento e) {
        EstadoPedidoSnapshot dto = new EstadoPedidoSnapshot(e.pedidoId(), EventoPedidoStream.VERSION_ACTUAL,
                e.cliente(), e.estado(), e.total(), e.fecha());
        enviar(props.topicos().estados().nombre(), dto.clave(), dto, headers("SNAPSHOT_ESTADO", e));
    }

    /** Headers de negocio: visibles en Kafka UI y útiles para filtrar sin deserializar el JSON. */
    private static Map<String, String> headers(String tipo, PedidoEvento e) {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("tipo-evento", tipo);
        h.put("estado", e.estado());
        h.put("origen", ORIGEN);
        h.put("version-esquema", String.valueOf(EventoPedidoStream.VERSION_ACTUAL));
        h.put("evento-id", e.eventoId().toString());
        return h;
    }

    private void enviar(String topico, String clave, Object dto, Map<String, String> headers) {
        try {
            productor.publicar(topico, clave, dto, headers);
        } catch (KafkaException error) {
            // send() falla de inmediato solo si no hay brokers disponibles (max.block.ms = 5 s).
            log.error("[KAFKA-SEND-ERROR] {} clave={} topic={} causa={}", dto.getClass().getSimpleName(), clave,
                    topico, error.getMessage());
        }
    }
}
