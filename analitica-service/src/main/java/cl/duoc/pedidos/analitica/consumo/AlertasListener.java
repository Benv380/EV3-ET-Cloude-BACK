package cl.duoc.pedidos.analitica.consumo;

import cl.duoc.pedidos.analitica.alertas.AlertasService;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.validacion.ValidadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * CONSUMIDOR del topic pedidos.eventos, grupo <b>analitica-alertas</b>, concurrencia 1:
 * un solo consumidor recibe las 3 particiones. Al ser otro grupo, recibe TODOS los eventos
 * independientemente del grupo de analítica (modelo publicar/suscribir).
 */
@Component
public class AlertasListener {

    private final ValidadorEventos validador;
    private final AlertasService alertas;

    public AlertasListener(ValidadorEventos validador, AlertasService alertas) {
        this.validador = validador;
        this.alertas = alertas;
    }

    @KafkaListener(id = "analitica-alertas",
            topics = "${streaming.topicos.eventos.nombre}",
            groupId = "${streaming.grupos.alertas}",
            concurrency = "1",
            properties = "spring.json.value.default.type=cl.duoc.pedidos.streaming.evento.EventoPedidoStream")
    public void recibir(ConsumerRecord<String, EventoPedidoStream> registro) {
        EventoPedidoStream evento = validador.validar(registro.value());
        alertas.evaluar(evento, MetadatosConsumo.de(registro, "analitica-alertas"));
    }
}
