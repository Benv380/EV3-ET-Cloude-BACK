package cl.duoc.pedidos.analitica.fraude;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

/**
 * Reglas de detección de posible fraude (analitica.fraude en application.yml).
 *
 * @param maxPedidos      pedidos permitidos por usuario dentro de la ventana; al superarlo se bloquea
 * @param ventanaSegundos tamaño de la ventana deslizante
 * @param montoMaximo     un pedido igual o mayor a este monto bloquea al usuario de inmediato
 * @param bloqueoMinutos  duración del bloqueo
 */
@Validated
@ConfigurationProperties(prefix = "analitica.fraude")
public record FraudeProperties(
        @Min(1) @Max(10000) int maxPedidos,
        @Min(1) @Max(3600) int ventanaSegundos,
        @NotNull @Positive BigDecimal montoMaximo,
        @Min(1) @Max(1440) int bloqueoMinutos) {
}
