package cl.duoc.pedidos.analitica.fraude;

import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.evento.TipoEventoPedido;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class DetectorFraudeTest {

    private final DetectorFraude detector =
            new DetectorFraude(new FraudeProperties(3, 60, new BigDecimal("10000000"), 15));
    private final Instant t0 = Instant.parse("2026-11-21T23:00:00Z");

    private EventoPedidoStream pedido(String usuario, String total, int segundo) {
        return new EventoPedidoStream(UUID.randomUUID(), 1, TipoEventoPedido.PEDIDO_CREADO, 1L, "Ana",
                new BigDecimal(total), "CREADO", null, usuario, "test", "NINGUNA", t0.plusSeconds(segundo));
    }

    @Test
    void rafagaSobreElMaximoBloquea() {
        assertThat(detector.evaluar(pedido("u1", "100", 0))).isEmpty();
        assertThat(detector.evaluar(pedido("u1", "100", 1))).isEmpty();
        assertThat(detector.evaluar(pedido("u1", "100", 2))).isEmpty();
        Optional<DecisionFraude> d = detector.evaluar(pedido("u1", "100", 3));
        assertThat(d).get().extracting(DecisionFraude::regla).isEqualTo(DecisionFraude.RAFAGA);
        assertThat(d.get().pedidosEnVentana()).isEqualTo(4);
    }

    @Test
    void laVentanaDeslizanteOlvidaLosPedidosAntiguos() {
        detector.evaluar(pedido("u1", "100", 0));
        detector.evaluar(pedido("u1", "100", 1));
        detector.evaluar(pedido("u1", "100", 2));
        assertThat(detector.evaluar(pedido("u1", "100", 120))).isEmpty();
    }

    @Test
    void cadaUsuarioTieneSuPropiaVentana() {
        for (int i = 0; i < 3; i++) {
            detector.evaluar(pedido("u1", "100", i));
        }
        assertThat(detector.evaluar(pedido("u2", "100", 3))).isEmpty();
    }

    @Test
    void montoExcesivoBloqueaDeInmediato() {
        assertThat(detector.evaluar(pedido("u3", "10000000", 0))).get()
                .extracting(DecisionFraude::regla).isEqualTo(DecisionFraude.MONTO_EXCESIVO);
    }

    @Test
    void soloEvaluaPedidosCreados() {
        EventoPedidoStream cambio = new EventoPedidoStream(UUID.randomUUID(), 1, TipoEventoPedido.ESTADO_CAMBIADO, 1L,
                "Ana", new BigDecimal("99999999"), "PAGADO", "CREADO", "u4", "test", "NINGUNA", t0);
        assertThat(detector.evaluar(cambio)).isEmpty();
    }

    @Test
    void reiniciarLimpiaLaVentana() {
        for (int i = 0; i < 3; i++) {
            detector.evaluar(pedido("u5", "100", i));
        }
        detector.reiniciar("u5");
        assertThat(detector.evaluar(pedido("u5", "100", 4))).isEmpty();
    }
}
