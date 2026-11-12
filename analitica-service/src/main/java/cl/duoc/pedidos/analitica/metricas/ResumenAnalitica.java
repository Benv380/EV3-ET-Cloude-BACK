package cl.duoc.pedidos.analitica.metricas;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/** Lo que muestra el dashboard de streaming. */
public record ResumenAnalitica(
        long totalEventos,
        Map<String, Long> porTipo,
        BigDecimal montoVendido,
        BigDecimal ticketPromedio,
        Map<String, Long> pedidosPorEstado,
        Map<Integer, Long> porParticion,
        Map<String, Long> porConsumidor,
        List<PuntoMinuto> eventosPorMinuto,
        long latenciaPromedioMs,
        long alertas,
        long mensajesDlt,
        List<EventoReciente> ultimos) {

    public record PuntoMinuto(String minuto, long eventos) {
    }

    public record EventoReciente(String tipo, Long pedidoId, String cliente, String estado, BigDecimal total,
                                 int particion, long offset, String consumidor, long latenciaMs,
                                 String fechaEvento) {
    }
}
