package cl.duoc.pedidos.analitica.alertas;

import cl.duoc.pedidos.analitica.config.AnaliticaProperties;
import cl.duoc.pedidos.analitica.consumo.MetadatosConsumo;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.evento.TipoEventoPedido;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Reglas de reacción en tiempo real. Consume el MISMO topic que la analítica pero en OTRO consumer group:
 * cada grupo recibe todos los eventos y avanza a su propio ritmo (uno no frena al otro).
 */
@Service
public class AlertasService {

    private static final Logger log = LoggerFactory.getLogger(AlertasService.class);

    private final AlertaStreamRepository repositorio;
    private final AnaliticaProperties props;

    public AlertasService(AlertaStreamRepository repositorio, AnaliticaProperties props) {
        this.repositorio = repositorio;
        this.props = props;
    }

    @Transactional
    public Optional<AlertaStream> evaluar(EventoPedidoStream e, MetadatosConsumo meta) {
        Optional<TipoAlerta> tipo = regla(e);
        if (tipo.isEmpty() || repositorio.existsByEventoId(e.eventoId())) {
            return Optional.empty();
        }
        String detalle = switch (tipo.get()) {
            case ALTO_MONTO -> "Pedido de $" + e.total().toPlainString() + " supera el umbral de $"
                    + props.umbralAltoMonto().toPlainString();
            case CANCELACION -> "Pedido cancelado (estaba " + e.estadoAnterior() + ")";
        };
        AlertaStream alerta = repositorio.save(new AlertaStream(e.eventoId(), tipo.get(), e.pedidoId(), e.cliente(),
                e.total(), detalle, meta.particion(), meta.offset(), e.fecha()));
        log.warn("[ALERTA] {} pedido={} {}", tipo.get(), e.pedidoId(), detalle);
        return Optional.of(alerta);
    }

    Optional<TipoAlerta> regla(EventoPedidoStream e) {
        if (e.tipo() == TipoEventoPedido.PEDIDO_CREADO && e.total().compareTo(props.umbralAltoMonto()) >= 0) {
            return Optional.of(TipoAlerta.ALTO_MONTO);
        }
        if (e.tipo() == TipoEventoPedido.ESTADO_CAMBIADO && "CANCELADO".equals(e.estado())) {
            return Optional.of(TipoAlerta.CANCELACION);
        }
        return Optional.empty();
    }

    @Transactional(readOnly = true)
    public List<AlertaStream> ultimas() {
        return repositorio.findTop50ByOrderByIdDesc();
    }

    @Transactional(readOnly = true)
    public long total() {
        return repositorio.count();
    }
}
