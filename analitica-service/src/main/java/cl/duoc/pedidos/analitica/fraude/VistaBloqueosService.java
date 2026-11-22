package cl.duoc.pedidos.analitica.fraude;

import cl.duoc.pedidos.streaming.evento.BloqueoUsuario;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Último estado de bloqueo de cada usuario, construido desde el topic compactado pedidos.bloqueos. */
@Service
public class VistaBloqueosService {

    private final Map<String, BloqueoUsuario> bloqueos = new ConcurrentHashMap<>();

    public void aplicar(BloqueoUsuario b) {
        bloqueos.merge(b.usuario(), b, (actual, nuevo) -> nuevo.desde().isBefore(actual.desde()) ? actual : nuevo);
    }

    public List<EstadoBloqueo> listar() {
        Instant ahora = Instant.now();
        return bloqueos.values().stream()
                .map(b -> new EstadoBloqueo(b.usuario(), b.vigente(ahora), b.regla(), b.motivo(), b.pedidosEnVentana(),
                        b.desde().toString(), b.hasta().toString()))
                .sorted(Comparator.comparing(EstadoBloqueo::vigente).reversed()
                        .thenComparing(EstadoBloqueo::desde, Comparator.reverseOrder()))
                .toList();
    }

    public long vigentes() {
        Instant ahora = Instant.now();
        return bloqueos.values().stream().filter(b -> b.vigente(ahora)).count();
    }

    public record EstadoBloqueo(String usuario, boolean vigente, String regla, String motivo, int pedidosEnVentana,
                                String desde, String hasta) {
    }
}
