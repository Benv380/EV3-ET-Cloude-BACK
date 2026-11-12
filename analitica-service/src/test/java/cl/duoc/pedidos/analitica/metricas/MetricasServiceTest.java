package cl.duoc.pedidos.analitica.metricas;

import cl.duoc.pedidos.analitica.consumo.MetadatosConsumo;
import cl.duoc.pedidos.streaming.error.ErrorTransitorioException;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.evento.TipoEventoPedido;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MetricasServiceTest {

    private final Instant ahora = Instant.parse("2026-11-12T15:30:20Z");
    private final EventoAnaliticoRepository repo = mock(EventoAnaliticoRepository.class);
    private final MetricasService servicio = new MetricasService(repo, Clock.fixed(ahora, ZoneOffset.UTC));
    private final MetadatosConsumo meta = new MetadatosConsumo("pedidos.eventos", 2, 41, "analitica-eventos", "consumidor-2");

    private EventoPedidoStream evento(String fallo) {
        return new EventoPedidoStream(UUID.randomUUID(), 1, TipoEventoPedido.PEDIDO_CREADO, 3L, "Ana",
                new BigDecimal("1000"), "CREADO", null, "ana", "test", fallo, ahora.minusMillis(250));
    }

    @Test
    void registraConParticionOffsetYLatencia() {
        when(repo.save(any())).thenAnswer(i -> i.getArgument(0));
        assertThat(servicio.registrar(evento("NINGUNA"), meta)).isTrue();
        verify(repo).save(org.mockito.ArgumentMatchers.argThat(e ->
                e.getParticion() == 2 && e.getOffset() == 41 && e.getLatenciaMs() == 250
                        && e.getConsumidor().equals("consumidor-2")));
    }

    @Test
    void duplicadoNoSeCuentaDosVeces() {
        when(repo.existsByEventoId(any())).thenReturn(true);
        assertThat(servicio.registrar(evento("NINGUNA"), meta)).isFalse();
        verify(repo, never()).save(any());
    }

    @Test
    void fallaSimuladaEsRecuperable() {
        assertThatThrownBy(() -> servicio.registrar(evento("ANALITICA"), meta))
                .isInstanceOf(ErrorTransitorioException.class);
    }

    @Test
    void serieDeMinutosSinHuecos() {
        EventoAnalitico e = new EventoAnalitico(UUID.randomUUID(), "PEDIDO_CREADO", 1L, "Ana", BigDecimal.ONE, "CREADO",
                null, "pedidos.eventos", 0, 1, "consumidor-0", ahora.minusSeconds(60), ahora);
        when(repo.findByFechaEventoAfter(any())).thenReturn(List.of(e));
        List<ResumenAnalitica.PuntoMinuto> serie = servicio.eventosPorMinuto(5);
        assertThat(serie).hasSize(5);
        assertThat(serie.get(3).eventos()).isEqualTo(1);
        assertThat(serie.stream().mapToLong(ResumenAnalitica.PuntoMinuto::eventos).sum()).isEqualTo(1);
    }
}
