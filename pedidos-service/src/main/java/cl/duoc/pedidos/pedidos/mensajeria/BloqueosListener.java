package cl.duoc.pedidos.pedidos.mensajeria;

import cl.duoc.pedidos.streaming.evento.BloqueoUsuario;
import cl.duoc.pedidos.streaming.validacion.ValidadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * CONSUMIDOR del topic compactado pedidos.bloqueos (grupo pedidos-bloqueos).
 * Al iniciar relee el topic desde el principio: gracias a la compactación obtiene el último estado de cada usuario.
 */
@Component
public class BloqueosListener implements ConsumerSeekAware {

    private final ValidadorEventos validador;
    private final RegistroBloqueosKafka registro;

    public BloqueosListener(ValidadorEventos validador, RegistroBloqueosKafka registro) {
        this.validador = validador;
        this.registro = registro;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> asignadas, ConsumerSeekCallback callback) {
        callback.seekToBeginning(asignadas.keySet());
    }

    @KafkaListener(id = "pedidos-bloqueos",
            topics = "${streaming.topicos.bloqueos.nombre}",
            groupId = "${streaming.grupos.bloqueos-pedidos}",
            concurrency = "1",
            properties = "spring.json.value.default.type=cl.duoc.pedidos.streaming.evento.BloqueoUsuario")
    public void recibir(ConsumerRecord<String, BloqueoUsuario> r) {
        if (r.value() != null) {
            registro.aplicar(validador.validar(r.value()));
        }
    }
}
