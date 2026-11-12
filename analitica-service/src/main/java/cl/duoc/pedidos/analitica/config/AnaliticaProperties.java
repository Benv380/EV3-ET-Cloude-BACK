package cl.duoc.pedidos.analitica.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

@Validated
@ConfigurationProperties(prefix = "analitica")
public record AnaliticaProperties(
        @NotNull @PositiveOrZero BigDecimal umbralAltoMonto,
        @Min(1) @Max(120) int ventanaMinutos) {
}
