package cl.duoc.pedidos.streaming.config;

import cl.duoc.pedidos.streaming.error.EventoInvalidoException;
import cl.duoc.pedidos.streaming.produccion.ProductorEventos;
import cl.duoc.pedidos.streaming.validacion.ValidadorEventos;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Validator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Infraestructura de Kafka compartida por los microservicios.
 * <ul>
 *   <li>Crea los topics del sistema con sus particiones, réplicas, retención y política de limpieza.</li>
 *   <li>Serializa los DTO a JSON con el ObjectMapper de Spring (fechas ISO-8601).</li>
 *   <li>Manejo de errores de los consumidores: reintentos con espera exponencial y, al agotarse,
 *       publicación en el Dead Letter Topic con el motivo del error en los headers.</li>
 * </ul>
 * Ninguna clase de negocio contiene configuración de Kafka.
 */
@AutoConfiguration(before = KafkaAutoConfiguration.class)
@EnableConfigurationProperties(StreamingProperties.class)
public class StreamingAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(StreamingAutoConfiguration.class);

    // ---------- Topics ----------

    /** KafkaAdmin los crea al iniciar si no existen (no modifica topics existentes). */
    @Bean
    @ConditionalOnProperty(prefix = "streaming", name = "crear-topicos", havingValue = "true", matchIfMissing = true)
    KafkaAdmin.NewTopics topicosPedidos(StreamingProperties props) {
        props.topicos().todos().forEach(t -> log.info("[KAFKA-TOPIC] {} particiones={} replicas={} configs={}",
                t.nombre(), t.particiones(), t.replicas(), t.configs()));
        return new KafkaAdmin.NewTopics(props.topicos().todos().stream()
                .map(StreamingProperties.Topico::nuevo).toArray(org.apache.kafka.clients.admin.NewTopic[]::new));
    }

    // ---------- Productor ----------

    /** Serializador JSON con el ObjectMapper de Spring Boot (Instant como texto ISO, no como número). */
    @Bean
    @SuppressWarnings({"unchecked", "rawtypes"})
    DefaultKafkaProducerFactoryCustomizer serializadorJson(ObjectMapper objectMapper) {
        return fabrica -> ((DefaultKafkaProducerFactory) fabrica)
                .setValueSerializerSupplier(() -> new JsonSerializer<>(objectMapper));
    }

    @Bean
    ValidadorEventos validadorEventos(Validator validator) {
        return new ValidadorEventos(validator);
    }

    @Bean
    ProductorEventos productorEventos(KafkaTemplate<Object, Object> kafkaTemplate, ValidadorEventos validador) {
        return new ProductorEventos(kafkaTemplate, validador);
    }

    // ---------- Manejo de errores de los consumidores ----------

    /**
     * Política de errores aplicada a TODOS los @KafkaListener (Spring Boot la registra en la fábrica de listeners):
     * <ol>
     *   <li>Error recuperable: se reintenta {@code reintentos.maximo} veces esperando 1 s, 2 s, 4 s...</li>
     *   <li>Error no recuperable (payload inválido o JSON corrupto): sin reintentos.</li>
     *   <li>Agotados los intentos: el registro se publica en el DLT con headers kafka_dlt-* (topic, partición
     *       y offset originales, clase y mensaje de la excepción) y el consumidor sigue con el siguiente.</li>
     * </ol>
     */
    @Bean
    @ConditionalOnMissingBean(CommonErrorHandler.class)
    DefaultErrorHandler manejadorErroresKafka(KafkaTemplate<Object, Object> kafkaTemplate,
                                              KafkaProperties kafkaProperties, StreamingProperties props) {
        // Registros cuyo JSON no se pudo leer llegan como bytes crudos: se reenvían tal cual al DLT.
        Map<String, Object> configBytes = new LinkedHashMap<>(kafkaProperties.buildProducerProperties(null));
        configBytes.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configBytes.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        KafkaTemplate<Object, Object> plantillaBytes = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configBytes));

        Map<Class<?>, KafkaOperations<?, ?>> plantillas = new LinkedHashMap<>();
        plantillas.put(byte[].class, plantillaBytes);
        plantillas.put(Object.class, kafkaTemplate);

        String dlt = props.topicos().dlt().nombre();
        // Partición -1: la elige Kafka según la clave (el DLT no necesita la misma partición que el original).
        DeadLetterPublishingRecoverer haciaDlt = new DeadLetterPublishingRecoverer(plantillas,
                (registro, error) -> new TopicPartition(dlt, -1));

        ConsumerRecordRecoverer recuperador = (registro, error) -> {
            log.error("[KAFKA-DLT] {}-{}@{} clave={} -> {} motivo={}", registro.topic(), registro.partition(),
                    registro.offset(), registro.key(), dlt, causaRaiz(error));
            haciaDlt.accept(registro, error);
        };

        StreamingProperties.Reintentos r = props.reintentos();
        ExponentialBackOffWithMaxRetries espera = new ExponentialBackOffWithMaxRetries(r.maximo());
        espera.setInitialInterval(r.esperaInicialMs());
        espera.setMultiplier(r.multiplicador());
        espera.setMaxInterval(r.esperaMaximaMs());

        DefaultErrorHandler manejador = new DefaultErrorHandler(recuperador, espera);
        manejador.addNotRetryableExceptions(EventoInvalidoException.class);
        manejador.setRetryListeners((registro, error, intento) ->
                log.warn("[KAFKA-ERROR] intento {} (máx. {}) {}-{}@{} clave={} motivo={}", intento, r.maximo() + 1,
                        registro.topic(), registro.partition(), registro.offset(), registro.key(), causaRaiz(error)));
        return manejador;
    }

    static String causaRaiz(Throwable error) {
        Throwable actual = error;
        while (actual.getCause() != null && actual.getCause() != actual) {
            actual = actual.getCause();
        }
        return actual.getClass().getSimpleName() + ": " + actual.getMessage();
    }
}
