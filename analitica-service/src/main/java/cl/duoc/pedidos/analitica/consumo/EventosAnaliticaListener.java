package cl.duoc.pedidos.analitica.consumo;

import cl.duoc.pedidos.analitica.metricas.MetricasService;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.validacion.ValidadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * CONSUMIDOR del topic pedidos.eventos, grupo <b>analitica-eventos</b>, concurrencia 3:
 * tres consumidores en el mismo grupo, Kafka asigna una partición a cada uno (balanceo de carga).
 * Si uno cae, sus particiones se reasignan a los otros (tolerancia a fallos).
 * <p>
 * Solo adapta Kafka → dominio: valida el payload y delega en {@link MetricasService}.
 * El ACK (commit del offset) lo hace el contenedor después de que este método termina sin error.
 */
@Component
public class EventosAnaliticaListener {

    private final ValidadorEventos validador;
    private final MetricasService metricas;

    public EventosAnaliticaListener(ValidadorEventos validador, MetricasService metricas) {
        this.validador = validador;
        this.metricas = metricas;
    }

    @KafkaListener(id = "analitica-eventos",
            topics = "${streaming.topicos.eventos.nombre}",
            groupId = "${streaming.grupos.analitica}",
            concurrency = "3",
            properties = "spring.json.value.default.type=cl.duoc.pedidos.streaming.evento.EventoPedidoStream")
    public void recibir(ConsumerRecord<String, EventoPedidoStream> registro) {
        EventoPedidoStream evento = validador.validar(registro.value());
        metricas.registrar(evento, MetadatosConsumo.de(registro, "analitica-eventos"));
    }
}
