package cl.duoc.pedidos.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    /** Cliente de administración de Kafka (mismos bootstrap servers que el resto de la configuración). */
    @Bean(destroyMethod = "close")
    AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    /**
     * Productor de TEXTO para mensajes de prueba: publica el valor tal cual (puede ser JSON inválido a propósito
     * para demostrar que el consumidor lo envía al DLT).
     */
    @Bean
    ProductorTexto productorTexto(KafkaProperties kafkaProperties) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new ProductorTexto(new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config)));
    }

    /**
     * Envoltorio del productor de texto. No se registra como KafkaTemplate para no reemplazar el
     * KafkaTemplate JSON que Spring Boot crea por defecto.
     */
    public record ProductorTexto(KafkaTemplate<String, String> template) {
    }
}
