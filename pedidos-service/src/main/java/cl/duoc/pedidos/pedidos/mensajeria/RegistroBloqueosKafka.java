package cl.duoc.pedidos.pedidos.mensajeria;

import cl.duoc.pedidos.pedidos.pedido.BloqueoVigente;
import cl.duoc.pedidos.pedidos.pedido.VerificadorBloqueos;
import cl.duoc.pedidos.streaming.evento.BloqueoUsuario;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Bloqueos vigentes en memoria, alimentados por el topic compactado pedidos.bloqueos. */
@Component
public class RegistroBloqueosKafka implements VerificadorBloqueos {

    private static final Logger log = LoggerFactory.getLogger(RegistroBloqueosKafka.class);

    private final Map<String, BloqueoUsuario> bloqueos = new ConcurrentHashMap<>();

    void aplicar(BloqueoUsuario b) {
        bloqueos.merge(b.usuario(), b, (actual, nuevo) -> nuevo.desde().isBefore(actual.desde()) ? actual : nuevo);
        log.info("[BLOQUEO] usuario={} activo={} regla={} hasta={}", b.usuario(), b.activo(), b.regla(), b.hasta());
    }

    @Override
    public Optional<BloqueoVigente> bloqueoVigente(String usuario) {
        BloqueoUsuario b = usuario == null ? null : bloqueos.get(usuario);
        if (b == null || !b.vigente(Instant.now())) {
            return Optional.empty();
        }
        return Optional.of(new BloqueoVigente(b.usuario(), b.regla(), b.motivo(), b.hasta()));
    }
}
