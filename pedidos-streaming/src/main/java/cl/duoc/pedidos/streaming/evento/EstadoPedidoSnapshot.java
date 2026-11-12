package cl.duoc.pedidos.streaming.evento;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * DTO del topic <b>pedidos.estados</b> (clave = pedidoId, cleanup.policy = compact).
 * <p>
 * No es un evento sino el ESTADO ACTUAL del pedido. Como el topic es compactado, Kafka termina guardando
 * solo el último snapshot de cada pedido: cualquier servicio puede reconstruir la vista completa leyendo
 * el topic desde el inicio.
 */
public record EstadoPedidoSnapshot(
        @NotNull @Positive Long pedidoId,
        @Positive int version,
        @NotBlank String cliente,
        @NotBlank String estado,
        @NotNull @PositiveOrZero BigDecimal total,
        @NotNull Instant actualizadoEn) {

    public String clave() {
        return String.valueOf(pedidoId);
    }
}
