package cl.duoc.pedidos.analitica.consumo;

import cl.duoc.pedidos.analitica.estados.ProyeccionEstadosService;
import cl.duoc.pedidos.streaming.evento.EstadoPedidoSnapshot;
import cl.duoc.pedidos.streaming.validacion.ValidadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * CONSUMIDOR del topic compactado pedidos.estados, grupo <b>analitica-estados</b>.
 * Al recibir sus particiones vuelve al offset inicial (seekToBeginning) para reconstruir la vista completa:
 * gracias a la compactación solo relee el último estado de cada pedido, no toda la historia.
 */
@Component
public class EstadosPedidoListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(EstadosPedidoListener.class);

    private final ValidadorEventos validador;
    private final ProyeccionEstadosService proyeccion;

    public EstadosPedidoListener(ValidadorEventos validador, ProyeccionEstadosService proyeccion) {
        this.validador = validador;
        this.proyeccion = proyeccion;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> asignadas, ConsumerSeekCallback callback) {
        log.info("[KAFKA-SEEK] reconstruyendo la vista de estados desde el inicio de {}", asignadas.keySet());
        callback.seekToBeginning(asignadas.keySet());
    }

    @KafkaListener(id = "analitica-estados",
            topics = "${streaming.topicos.estados.nombre}",
            groupId = "${streaming.grupos.estados}",
            concurrency = "3",
            properties = "spring.json.value.default.type=cl.duoc.pedidos.streaming.evento.EstadoPedidoSnapshot")
    public void recibir(ConsumerRecord<String, EstadoPedidoSnapshot> registro) {
        if (registro.value() == null) {
            proyeccion.eliminar(Long.valueOf(registro.key()));
            return;
        }
        proyeccion.aplicar(validador.validar(registro.value()));
    }
}
