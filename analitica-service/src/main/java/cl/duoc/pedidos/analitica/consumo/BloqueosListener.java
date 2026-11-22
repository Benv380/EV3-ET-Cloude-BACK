package cl.duoc.pedidos.analitica.consumo;

import cl.duoc.pedidos.analitica.fraude.VistaBloqueosService;
import cl.duoc.pedidos.streaming.evento.BloqueoUsuario;
import cl.duoc.pedidos.streaming.validacion.ValidadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/** CONSUMIDOR del topic compactado pedidos.bloqueos (grupo analitica-bloqueos): vista para el dashboard. */
@Component
public class BloqueosListener implements ConsumerSeekAware {

    private final ValidadorEventos validador;
    private final VistaBloqueosService vista;

    public BloqueosListener(ValidadorEventos validador, VistaBloqueosService vista) {
        this.validador = validador;
        this.vista = vista;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> asignadas, ConsumerSeekCallback callback) {
        callback.seekToBeginning(asignadas.keySet());
    }

    @KafkaListener(id = "analitica-bloqueos",
            topics = "${streaming.topicos.bloqueos.nombre}",
            groupId = "${streaming.grupos.bloqueos-analitica}",
            concurrency = "1",
            properties = "spring.json.value.default.type=cl.duoc.pedidos.streaming.evento.BloqueoUsuario")
    public void recibir(ConsumerRecord<String, BloqueoUsuario> registro) {
        if (registro.value() != null) {
            vista.aplicar(validador.validar(registro.value()));
        }
    }
}
