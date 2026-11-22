package cl.duoc.pedidos.analitica.consumo;

import cl.duoc.pedidos.analitica.alertas.AlertasService;
import cl.duoc.pedidos.analitica.fraude.FraudeService;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.validacion.ValidadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * CONSUMIDOR del topic pedidos.eventos, grupo <b>analitica-alertas</b>, concurrencia 1:
 * un solo consumidor recibe las 3 particiones. Al ser otro grupo, recibe TODOS los eventos
 * independientemente del grupo de analítica (modelo publicar/suscribir).
 * También aplica las reglas de POSIBLE FRAUDE: al tener las 3 particiones ve todos los pedidos de cada usuario.
 */
@Component
public class AlertasListener {

    private final ValidadorEventos validador;
    private final AlertasService alertas;
    private final FraudeService fraude;

    public AlertasListener(ValidadorEventos validador, AlertasService alertas, FraudeService fraude) {
        this.validador = validador;
        this.alertas = alertas;
        this.fraude = fraude;
    }

    @KafkaListener(id = "analitica-alertas",
            topics = "${streaming.topicos.eventos.nombre}",
            groupId = "${streaming.grupos.alertas}",
            concurrency = "1",
            properties = "spring.json.value.default.type=cl.duoc.pedidos.streaming.evento.EventoPedidoStream")
    public void recibir(ConsumerRecord<String, EventoPedidoStream> registro) {
        EventoPedidoStream evento = validador.validar(registro.value());
        MetadatosConsumo meta = MetadatosConsumo.de(registro, "analitica-alertas");
        alertas.evaluar(evento, meta);
        fraude.evaluar(evento, meta);
    }
}
