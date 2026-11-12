package cl.duoc.pedidos.streaming.evento;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * DTO del topic <b>pedidos.eventos</b> (clave = pedidoId).
 * <p>
 * Es un "hecho" inmutable: algo que ya ocurrió. Está pensado para analítica, por eso NO incluye datos personales
 * sensibles (correo del cliente) y SÍ incluye lo necesario para calcular métricas (tipo, total, estado, fecha).
 * {@code version} permite evolucionar el esquema sin romper a los consumidores existentes.
 */
public record EventoPedidoStream(
        @NotNull UUID eventoId,
        @Positive int version,
        @NotNull TipoEventoPedido tipo,
        @NotNull @Positive Long pedidoId,
        @NotBlank @Size(max = 120) String cliente,
        @NotNull @PositiveOrZero BigDecimal total,
        @NotBlank String estado,
        String estadoAnterior,
        @Size(max = 160) String usuario,
        @NotBlank String origen,
        String simularFallo,
        @NotNull Instant fecha) {

    public static final int VERSION_ACTUAL = 1;

    /** Clave del registro: todos los eventos de un pedido quedan ordenados en la misma partición. */
    public String clave() {
        return String.valueOf(pedidoId);
    }

    public boolean simula(String fallo) {
        return fallo.equalsIgnoreCase(simularFallo);
    }
}
