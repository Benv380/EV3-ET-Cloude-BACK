package cl.duoc.pedidos.analitica.estados;

import cl.duoc.pedidos.streaming.evento.EstadoPedidoSnapshot;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Vista en memoria con el estado ACTUAL de cada pedido, construida solo desde el topic compactado pedidos.estados.
 * <p>
 * No usa base de datos: al iniciar, el consumidor relee el topic desde el inicio y la vista queda reconstruida.
 * Eso funciona porque la compactación deja en Kafka el último snapshot de cada pedido (clave = pedidoId).
 */
@Service
public class ProyeccionEstadosService {

    private final Map<Long, EstadoPedidoSnapshot> estados = new ConcurrentHashMap<>();
    private final AtomicLong registrosLeidos = new AtomicLong();

    /** Aplica un snapshot. Si llega uno más antiguo que el que ya hay (reproceso), se ignora. */
    public void aplicar(EstadoPedidoSnapshot snapshot) {
        registrosLeidos.incrementAndGet();
        estados.merge(snapshot.pedidoId(), snapshot,
                (actual, nuevo) -> nuevo.actualizadoEn().isBefore(actual.actualizadoEn()) ? actual : nuevo);
    }

    /** Tombstone (valor null): el pedido se elimina de la vista y Kafka lo borrará en la próxima compactación. */
    public void eliminar(Long pedidoId) {
        registrosLeidos.incrementAndGet();
        estados.remove(pedidoId);
    }

    public Map<String, Long> conteoPorEstado() {
        Map<String, Long> conteo = new TreeMap<>();
        estados.values().forEach(s -> conteo.merge(s.estado(), 1L, Long::sum));
        return conteo;
    }

    public List<EstadoPedidoSnapshot> listar() {
        return estados.values().stream()
                .sorted(Comparator.comparing(EstadoPedidoSnapshot::actualizadoEn).reversed())
                .limit(100)
                .toList();
    }

    public VistaEstados vista() {
        return new VistaEstados(estados.size(), registrosLeidos.get(), conteoPorEstado(), listar());
    }

    public record VistaEstados(int pedidos, long registrosLeidos, Map<String, Long> porEstado,
                               List<EstadoPedidoSnapshot> ultimos) {
    }
}
