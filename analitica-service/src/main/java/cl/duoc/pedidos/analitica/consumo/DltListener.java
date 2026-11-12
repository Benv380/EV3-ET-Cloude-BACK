package cl.duoc.pedidos.analitica.consumo;

import cl.duoc.pedidos.analitica.dlt.MensajeDlt;
import cl.duoc.pedidos.analitica.dlt.MonitorDltService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/**
 * CONSUMIDOR del Dead Letter Topic (grupo <b>monitor-dlt</b>). Lee el valor como texto (puede ser un JSON
 * corrupto) y los headers kafka_dlt-* que agrega el manejador de errores con el motivo de la falla.
 */
@Component
public class DltListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(DltListener.class);

    private final MonitorDltService monitor;

    public DltListener(MonitorDltService monitor) {
        this.monitor = monitor;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> asignadas, ConsumerSeekCallback callback) {
        callback.seekToBeginning(asignadas.keySet());
    }

    @KafkaListener(id = "monitor-dlt",
            topics = "${streaming.topicos.dlt.nombre}",
            groupId = "${streaming.grupos.dlt}",
            concurrency = "1",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    public void recibir(ConsumerRecord<String, String> registro) {
        MensajeDlt mensaje = new MensajeDlt(registro.partition(), registro.offset(), registro.key(),
                texto(registro, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                entero(registro, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                largo(registro, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                texto(registro, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
                simple(texto(registro, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN) != null
                        ? texto(registro, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)
                        : texto(registro, KafkaHeaders.DLT_EXCEPTION_FQCN)),
                motivo(texto(registro, KafkaHeaders.DLT_EXCEPTION_MESSAGE)),
                recortar(registro.value()),
                Instant.ofEpochMilli(registro.timestamp()).toString());
        monitor.registrar(mensaje);
        log.info("[DLT] {}-{}@{} grupo={} motivo={}", mensaje.topicoOriginal(), mensaje.particionOriginal(),
                mensaje.offsetOriginal(), mensaje.grupoOriginal(), mensaje.motivo());
    }

    private static String texto(ConsumerRecord<?, ?> r, String nombre) {
        Header h = r.headers().lastHeader(nombre);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static Integer entero(ConsumerRecord<?, ?> r, String nombre) {
        Header h = r.headers().lastHeader(nombre);
        return h == null || h.value().length != Integer.BYTES ? null : ByteBuffer.wrap(h.value()).getInt();
    }

    private static Long largo(ConsumerRecord<?, ?> r, String nombre) {
        Header h = r.headers().lastHeader(nombre);
        return h == null || h.value().length != Long.BYTES ? null : ByteBuffer.wrap(h.value()).getLong();
    }

    /** Spring antepone "Listener method '...' threw exception; " al mensaje real: se deja solo la causa. */
    static String motivo(String mensaje) {
        if (mensaje == null) {
            return null;
        }
        int i = mensaje.lastIndexOf("threw exception; ");
        return i >= 0 ? mensaje.substring(i + "threw exception; ".length()) : mensaje;
    }

    private static String simple(String fqcn) {
        return fqcn == null ? null : fqcn.substring(fqcn.lastIndexOf('.') + 1);
    }

    private static String recortar(String valor) {
        if (valor == null) {
            return null;
        }
        return valor.length() > 600 ? valor.substring(0, 600) + "..." : valor;
    }
}
