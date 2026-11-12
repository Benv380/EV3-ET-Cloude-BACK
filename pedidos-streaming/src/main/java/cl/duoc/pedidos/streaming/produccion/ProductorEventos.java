package cl.duoc.pedidos.streaming.produccion;

import cl.duoc.pedidos.streaming.validacion.ValidadorEventos;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * PRODUCTOR genérico de Kafka.
 * <ol>
 *   <li>Valida el DTO antes de enviarlo: un evento inválido nunca llega al topic.</li>
 *   <li>Envía con clave: misma clave = misma partición = orden garantizado por pedido.</li>
 *   <li>Agrega headers de negocio (tipo de evento, estado, origen, versión del esquema): en Kafka UI se ven
 *       junto a cada mensaje y permiten filtrar sin abrir el JSON.</li>
 *   <li>Registra en el log la partición y el offset asignados por el broker, o el error.</li>
 * </ol>
 * Los reintentos de red los hace el propio cliente Kafka (retries + idempotencia, ver streaming.yml).
 */
public class ProductorEventos {

    private static final Logger log = LoggerFactory.getLogger(ProductorEventos.class);

    private final KafkaTemplate<Object, Object> template;
    private final ValidadorEventos validador;

    public ProductorEventos(KafkaTemplate<Object, Object> template, ValidadorEventos validador) {
        this.template = template;
        this.validador = validador;
    }

    public CompletableFuture<SendResult<Object, Object>> publicar(String topico, String clave, Object evento) {
        return publicar(topico, clave, evento, Map.of());
    }

    public CompletableFuture<SendResult<Object, Object>> publicar(String topico, String clave, Object evento,
                                                                  Map<String, String> headers) {
        validador.validar(evento);
        ProducerRecord<Object, Object> registro = new ProducerRecord<>(topico, clave, evento);
        headers.forEach((nombre, valor) -> {
            if (valor != null) {
                registro.headers().add(nombre, valor.getBytes(StandardCharsets.UTF_8));
            }
        });
        String tipo = headers.getOrDefault("tipo-evento", evento.getClass().getSimpleName());
        return template.send(registro).whenComplete((resultado, error) -> {
            if (error != null) {
                log.error("[KAFKA-SEND-ERROR] {} clave={} topic={} causa={}", tipo, clave, topico, error.getMessage());
            } else {
                var meta = resultado.getRecordMetadata();
                log.info("[KAFKA-SEND] {} clave={} -> {}-{} offset={}", tipo, clave, meta.topic(),
                        meta.partition(), meta.offset());
            }
        });
    }
}
