package cl.duoc.pedidos.analitica.fraude;

import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.evento.TipoEventoPedido;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Reglas de posible fraude, sin dependencias de Kafka (se prueba con datos de ejemplo).
 * <ul>
 *   <li><b>RAFAGA_DE_PEDIDOS</b>: un mismo usuario crea más de {@code maxPedidos} pedidos dentro de una ventana
 *       deslizante de {@code ventanaSegundos} (típico de un script o de una cuenta comprometida).</li>
 *   <li><b>MONTO_EXCESIVO</b>: un pedido igual o mayor a {@code montoMaximo}.</li>
 * </ul>
 * La ventana usa la fecha del evento (no la hora de procesamiento), así el resultado no depende del lag.
 * Funciona porque el grupo analitica-alertas tiene UN consumidor que recibe las 3 particiones: ve todos los
 * pedidos de cada usuario aunque, por su clave (pedidoId), queden en particiones distintas.
 */
public class DetectorFraude {

    private final FraudeProperties props;
    private final Map<String, Deque<Instant>> ventanas = new HashMap<>();

    public DetectorFraude(FraudeProperties props) {
        this.props = props;
    }

    public synchronized Optional<DecisionFraude> evaluar(EventoPedidoStream e) {
        if (e.tipo() != TipoEventoPedido.PEDIDO_CREADO || e.usuario() == null || e.usuario().isBlank()) {
            return Optional.empty();
        }
        Deque<Instant> ventana = ventanas.computeIfAbsent(e.usuario(), u -> new ArrayDeque<>());
        Instant limite = e.fecha().minus(Duration.ofSeconds(props.ventanaSegundos()));
        ventana.removeIf(t -> t.isBefore(limite));
        ventana.addLast(e.fecha());
        int cantidad = ventana.size();

        if (e.total().compareTo(props.montoMaximo()) >= 0) {
            return Optional.of(new DecisionFraude(DecisionFraude.MONTO_EXCESIVO,
                    "Pedido de $" + e.total().toPlainString() + " supera el máximo permitido de $"
                            + props.montoMaximo().toPlainString(), cantidad));
        }
        if (cantidad > props.maxPedidos()) {
            return Optional.of(new DecisionFraude(DecisionFraude.RAFAGA,
                    cantidad + " pedidos en " + props.ventanaSegundos() + " s (máximo " + props.maxPedidos() + ")",
                    cantidad));
        }
        return Optional.empty();
    }

    /** Al desbloquear manualmente se reinicia la ventana del usuario. */
    public synchronized void reiniciar(String usuario) {
        ventanas.remove(usuario);
    }
}
