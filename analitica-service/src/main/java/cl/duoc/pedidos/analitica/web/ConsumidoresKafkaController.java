package cl.duoc.pedidos.analitica.web;

import cl.duoc.pedidos.common.web.RecursoNoEncontradoException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Pausar / reanudar consumidores Kafka para la demo: al pausar, los mensajes se acumulan (sube el LAG del grupo)
 * sin perderse; al reanudar, el consumidor continúa desde el último offset confirmado.
 */
@RestController
@RequestMapping("/api/analitica/consumidores")
public class ConsumidoresKafkaController {

    private static final Logger log = LoggerFactory.getLogger(ConsumidoresKafkaController.class);

    private final KafkaListenerEndpointRegistry registro;

    public ConsumidoresKafkaController(KafkaListenerEndpointRegistry registro) {
        this.registro = registro;
    }

    @GetMapping
    public List<EstadoConsumidor> listar() {
        return registro.getListenerContainers().stream()
                .map(ConsumidoresKafkaController::estado)
                .sorted(Comparator.comparing(EstadoConsumidor::id))
                .toList();
    }

    @PostMapping("/{id}/pausar")
    public EstadoConsumidor pausar(@PathVariable String id) {
        MessageListenerContainer c = contenedor(id);
        c.pause();
        log.warn("[CONSUMIDOR] {} PAUSADO: los mensajes se acumulan en Kafka (aumenta el lag)", id);
        return estado(c);
    }

    @PostMapping("/{id}/reanudar")
    public EstadoConsumidor reanudar(@PathVariable String id) {
        MessageListenerContainer c = contenedor(id);
        c.resume();
        log.info("[CONSUMIDOR] {} REANUDADO: continúa desde el último offset confirmado", id);
        return estado(c);
    }

    private MessageListenerContainer contenedor(String id) {
        MessageListenerContainer c = registro.getListenerContainer(id);
        if (c == null) {
            throw new RecursoNoEncontradoException("No existe el consumidor " + id);
        }
        return c;
    }

    private static EstadoConsumidor estado(MessageListenerContainer c) {
        Collection<TopicPartition> asignadas = c.getAssignedPartitions();
        List<String> particiones = asignadas == null ? List.of()
                : asignadas.stream().map(tp -> tp.topic() + "-" + tp.partition()).sorted().toList();
        String[] topicos = c.getContainerProperties().getTopics();
        return new EstadoConsumidor(c.getListenerId(), c.getGroupId(),
                topicos == null ? List.of() : List.of(topicos), c.isRunning(),
                c.isPauseRequested() || c.isContainerPaused(), particiones);
    }

    public record EstadoConsumidor(String id, String grupo, List<String> topicos, boolean activo, boolean pausado,
                                   List<String> particiones) {
    }
}
