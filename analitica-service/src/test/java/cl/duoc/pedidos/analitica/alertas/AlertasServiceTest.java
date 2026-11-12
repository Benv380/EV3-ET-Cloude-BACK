package cl.duoc.pedidos.analitica.alertas;

import cl.duoc.pedidos.analitica.config.AnaliticaProperties;
import cl.duoc.pedidos.analitica.consumo.MetadatosConsumo;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.evento.TipoEventoPedido;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AlertasServiceTest {

    private final AlertaStreamRepository repo = mock(AlertaStreamRepository.class);
    private final AlertasService servicio =
            new AlertasService(repo, new AnaliticaProperties(new BigDecimal("1000000"), 15));
    private final MetadatosConsumo meta = new MetadatosConsumo("pedidos.eventos", 1, 10, "analitica-alertas", "consumidor-0");

    private static EventoPedidoStream e(TipoEventoPedido tipo, String total, String estado, String anterior) {
        return new EventoPedidoStream(UUID.randomUUID(), 1, tipo, 5L, "Ana", new BigDecimal(total), estado, anterior,
                "ana", "test", "NINGUNA", Instant.now());
    }

    @Test
    void altoMontoGeneraAlerta() {
        when(repo.save(any())).thenAnswer(i -> i.getArgument(0));
        assertThat(servicio.evaluar(e(TipoEventoPedido.PEDIDO_CREADO, "1000000", "CREADO", null), meta))
                .get().extracting(AlertaStream::getTipo).isEqualTo("ALTO_MONTO");
    }

    @Test
    void cancelacionGeneraAlerta() {
        when(repo.save(any())).thenAnswer(i -> i.getArgument(0));
        assertThat(servicio.evaluar(e(TipoEventoPedido.ESTADO_CAMBIADO, "100", "CANCELADO", "PAGADO"), meta))
                .get().extracting(AlertaStream::getTipo).isEqualTo("CANCELACION");
    }

    @Test
    void pedidoNormalNoGeneraAlerta() {
        assertThat(servicio.evaluar(e(TipoEventoPedido.PEDIDO_CREADO, "999999", "CREADO", null), meta)).isEmpty();
        verify(repo, never()).save(any());
    }

    @Test
    void eventoRepetidoNoDuplicaLaAlerta() {
        when(repo.existsByEventoId(any())).thenReturn(true);
        assertThat(servicio.evaluar(e(TipoEventoPedido.PEDIDO_CREADO, "5000000", "CREADO", null), meta)).isEmpty();
        verify(repo, never()).save(any());
    }
}
