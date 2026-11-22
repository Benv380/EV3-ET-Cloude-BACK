package cl.duoc.pedidos.analitica.fraude;

import cl.duoc.pedidos.analitica.alertas.AlertaStream;
import cl.duoc.pedidos.analitica.alertas.AlertaStreamRepository;
import cl.duoc.pedidos.analitica.alertas.TipoAlerta;
import cl.duoc.pedidos.analitica.consumo.MetadatosConsumo;
import cl.duoc.pedidos.streaming.config.StreamingProperties;
import cl.duoc.pedidos.streaming.evento.BloqueoUsuario;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.produccion.ProductorEventos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reacción en tiempo real ante un posible fraude: registra la alerta y publica el BLOQUEO del usuario en el
 * topic compactado pedidos.bloqueos. pedidos-service lo consume y rechaza los pedidos siguientes (403).
 */
@Service
public class FraudeService {

    private static final Logger log = LoggerFactory.getLogger(FraudeService.class);

    private final DetectorFraude detector;
    private final FraudeProperties props;
    private final ProductorEventos productor;
    private final StreamingProperties streaming;
    private final AlertaStreamRepository alertas;
    private final Clock reloj = Clock.systemUTC();
    /** Bloqueos publicados por esta instancia (evita publicar uno nuevo por cada pedido de la misma ráfaga). */
    private final Map<String, Instant> bloqueadosHasta = new ConcurrentHashMap<>();

    public FraudeService(FraudeProperties props, ProductorEventos productor, StreamingProperties streaming,
                         AlertaStreamRepository alertas) {
        this.detector = new DetectorFraude(props);
        this.props = props;
        this.productor = productor;
        this.streaming = streaming;
        this.alertas = alertas;
    }

    @Transactional
    public Optional<BloqueoUsuario> evaluar(EventoPedidoStream e, MetadatosConsumo meta) {
        Optional<DecisionFraude> decision = detector.evaluar(e);
        if (decision.isEmpty()) {
            return Optional.empty();
        }
        Instant ahora = reloj.instant();
        Instant vigente = bloqueadosHasta.get(e.usuario());
        if (vigente != null && ahora.isBefore(vigente)) {
            return Optional.empty();
        }
        DecisionFraude d = decision.get();
        BloqueoUsuario bloqueo = new BloqueoUsuario(e.usuario(), true, d.regla(), d.motivo(), d.pedidosEnVentana(),
                ahora, ahora.plus(Duration.ofMinutes(props.bloqueoMinutos())), 1);
        publicar(bloqueo, "BLOQUEO_FRAUDE");
        bloqueadosHasta.put(e.usuario(), bloqueo.hasta());

        UUID idAlerta = UUID.nameUUIDFromBytes(("fraude-" + e.eventoId()).getBytes(StandardCharsets.UTF_8));
        if (!alertas.existsByEventoId(idAlerta)) {
            alertas.save(new AlertaStream(idAlerta, TipoAlerta.FRAUDE, e.pedidoId(), e.cliente(), e.total(),
                    "Posible fraude (" + d.regla() + "): " + d.motivo() + ". Usuario bloqueado "
                            + props.bloqueoMinutos() + " min", meta.particion(), meta.offset(), e.fecha()));
        }
        log.warn("[FRAUDE] usuario={} regla={} motivo='{}' -> BLOQUEADO hasta {}", e.usuario(), d.regla(),
                d.motivo(), bloqueo.hasta());
        return Optional.of(bloqueo);
    }

    /** Desbloqueo manual (administrador): publica activo=false para el usuario en el topic compactado. */
    public BloqueoUsuario desbloquear(String usuario) {
        Instant ahora = reloj.instant();
        BloqueoUsuario desbloqueo = new BloqueoUsuario(usuario, false, "MANUAL", "Desbloqueado por un administrador",
                0, ahora, ahora, 1);
        publicar(desbloqueo, "DESBLOQUEO");
        bloqueadosHasta.remove(usuario);
        detector.reiniciar(usuario);
        log.info("[FRAUDE] usuario={} DESBLOQUEADO manualmente", usuario);
        return desbloqueo;
    }

    private void publicar(BloqueoUsuario b, String tipo) {
        productor.publicar(streaming.topicos().bloqueos().nombre(), b.usuario(), b,
                Map.of("tipo-evento", tipo, "regla", b.regla(), "origen", "analitica-service"));
    }
}
