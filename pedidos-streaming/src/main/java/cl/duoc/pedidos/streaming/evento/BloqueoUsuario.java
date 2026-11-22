package cl.duoc.pedidos.streaming.evento;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;

/**
 * DTO del topic <b>pedidos.bloqueos</b> (clave = usuario, cleanup.policy = compact).
 * <p>
 * Lo publica analitica-service cuando detecta un posible fraude, y también al desbloquear (activo = false).
 * Como el topic es compactado, Kafka conserva el ÚLTIMO estado de bloqueo de cada usuario: pedidos-service
 * lo relee al iniciar y siempre sabe quién está bloqueado.
 *
 * @param regla            RAFAGA_DE_PEDIDOS, MONTO_EXCESIVO o MANUAL (desbloqueo)
 * @param pedidosEnVentana pedidos del usuario dentro de la ventana de tiempo al momento de bloquear
 */
public record BloqueoUsuario(
        @NotBlank String usuario,
        boolean activo,
        @NotBlank String regla,
        @NotBlank String motivo,
        @PositiveOrZero int pedidosEnVentana,
        @NotNull Instant desde,
        @NotNull Instant hasta,
        int version) {

    public boolean vigente(Instant ahora) {
        return activo && ahora.isBefore(hasta);
    }
}
