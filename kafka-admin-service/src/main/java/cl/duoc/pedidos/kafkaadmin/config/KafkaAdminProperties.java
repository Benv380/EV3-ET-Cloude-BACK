package cl.duoc.pedidos.kafkaadmin.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "kafka-admin")
public record KafkaAdminProperties(
        @Min(1) @Max(9) int brokersEsperados,
        @Min(1) @Max(100) int maxParticiones,
        @Min(1) long umbralLag,
        @Min(1000) long timeoutMs) {
}
