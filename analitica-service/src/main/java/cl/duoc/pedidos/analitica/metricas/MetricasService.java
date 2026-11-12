package cl.duoc.pedidos.analitica.metricas;

import cl.duoc.pedidos.analitica.consumo.MetadatosConsumo;
import cl.duoc.pedidos.streaming.error.ErrorTransitorioException;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.evento.TipoEventoPedido;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Lógica de negocio de la analítica en tiempo real. No conoce Kafka: recibe el DTO ya validado y los metadatos.
 * <p>
 * Idempotente: si Kafka entrega dos veces el mismo evento (por ejemplo tras un rebalanceo antes de confirmar
 * el offset) se detecta por eventoId y no se cuenta dos veces.
 */
@Service
public class MetricasService {

    private static final Logger log = LoggerFactory.getLogger(MetricasService.class);
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm").withZone(ZoneId.of("America/Santiago"));

    private final EventoAnaliticoRepository repositorio;
    private final Clock reloj;

    @Autowired
    public MetricasService(EventoAnaliticoRepository repositorio) {
        this(repositorio, Clock.systemUTC());
    }

    MetricasService(EventoAnaliticoRepository repositorio, Clock reloj) {
        this.repositorio = repositorio;
        this.reloj = reloj;
    }

    /** @return true si el evento se registró; false si era un duplicado. */
    @Transactional
    public boolean registrar(EventoPedidoStream evento, MetadatosConsumo meta) {
        if (evento.simula("ANALITICA")) {
            throw new ErrorTransitorioException("SIMULADO: la base de datos de analítica no responde");
        }
        if (repositorio.existsByEventoId(evento.eventoId())) {
            log.info("[STREAM-DUPLICADO] evento {} ya procesado; se ignora", evento.eventoId());
            return false;
        }
        EventoAnalitico registro = repositorio.save(new EventoAnalitico(evento.eventoId(), evento.tipo().name(),
                evento.pedidoId(), evento.cliente(), evento.total(), evento.estado(), evento.estadoAnterior(),
                meta.topico(), meta.particion(), meta.offset(), meta.consumidor(), evento.fecha(), reloj.instant()));
        log.info("[STREAM] {} pedido={} {}-{}@{} {} latencia={}ms", evento.tipo(), evento.pedidoId(), meta.topico(),
                meta.particion(), meta.offset(), meta.consumidor(), registro.getLatenciaMs());
        return true;
    }

    @Transactional(readOnly = true)
    public ResumenAnalitica resumen(Map<String, Long> pedidosPorEstado, long alertas, long mensajesDlt,
                                    int ventanaMinutos) {
        Map<String, Long> porTipo = new LinkedHashMap<>();
        for (TipoEventoPedido tipo : TipoEventoPedido.values()) {
            porTipo.put(tipo.name(), 0L);
        }
        repositorio.contarPorTipo().forEach(f -> porTipo.put((String) f[0], (Long) f[1]));
        long total = porTipo.values().stream().mapToLong(Long::longValue).sum();

        BigDecimal monto = repositorio.montoCreado();
        long creados = porTipo.getOrDefault(TipoEventoPedido.PEDIDO_CREADO.name(), 0L);
        BigDecimal ticket = creados == 0 ? BigDecimal.ZERO
                : monto.divide(BigDecimal.valueOf(creados), 0, RoundingMode.HALF_UP);

        Map<Integer, Long> porParticion = new TreeMap<>();
        repositorio.contarPorParticion().forEach(f -> porParticion.put((Integer) f[0], (Long) f[1]));
        Map<String, Long> porConsumidor = new TreeMap<>();
        repositorio.contarPorConsumidor().forEach(f -> porConsumidor.put((String) f[0], (Long) f[1]));

        List<EventoAnalitico> recientes = repositorio.findTop25ByOrderByIdDesc();
        long latencia = Math.round(recientes.stream().mapToLong(EventoAnalitico::getLatenciaMs).average().orElse(0));

        return new ResumenAnalitica(total, porTipo, monto, ticket, pedidosPorEstado, porParticion, porConsumidor,
                eventosPorMinuto(ventanaMinutos), latencia, alertas, mensajesDlt,
                recientes.stream().map(MetricasService::reciente).toList());
    }

    /** Serie de los últimos N minutos (incluye minutos sin eventos, para que el gráfico no tenga huecos). */
    List<ResumenAnalitica.PuntoMinuto> eventosPorMinuto(int minutos) {
        Instant ahora = reloj.instant().truncatedTo(ChronoUnit.MINUTES);
        Instant desde = ahora.minus(minutos - 1L, ChronoUnit.MINUTES);
        Map<Instant, Long> conteo = new TreeMap<>();
        for (int i = 0; i < minutos; i++) {
            conteo.put(desde.plus(i, ChronoUnit.MINUTES), 0L);
        }
        repositorio.findByFechaEventoAfter(desde.minusMillis(1)).forEach(e ->
                conteo.computeIfPresent(e.getFechaEvento().truncatedTo(ChronoUnit.MINUTES), (k, v) -> v + 1));
        List<ResumenAnalitica.PuntoMinuto> serie = new ArrayList<>();
        conteo.forEach((minuto, n) -> serie.add(new ResumenAnalitica.PuntoMinuto(HORA.format(minuto), n)));
        return serie;
    }

    private static ResumenAnalitica.EventoReciente reciente(EventoAnalitico e) {
        return new ResumenAnalitica.EventoReciente(e.getTipo(), e.getPedidoId(), e.getCliente(), e.getEstado(),
                e.getTotal(), e.getParticion(), e.getOffset(), e.getConsumidor(), e.getLatenciaMs(),
                e.getFechaEvento().toString());
    }
}
