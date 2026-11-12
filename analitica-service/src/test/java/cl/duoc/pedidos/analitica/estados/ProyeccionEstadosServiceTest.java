package cl.duoc.pedidos.analitica.estados;

import cl.duoc.pedidos.streaming.evento.EstadoPedidoSnapshot;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ProyeccionEstadosServiceTest {

    private final ProyeccionEstadosService vista = new ProyeccionEstadosService();
    private final Instant t0 = Instant.parse("2026-11-12T10:00:00Z");

    private EstadoPedidoSnapshot s(long id, String estado, int segundos) {
        return new EstadoPedidoSnapshot(id, 1, "Ana", estado, BigDecimal.TEN, t0.plusSeconds(segundos));
    }

    @Test
    void conservaSoloElUltimoEstadoDeCadaPedido() {
        vista.aplicar(s(1, "CREADO", 0));
        vista.aplicar(s(1, "PAGADO", 10));
        vista.aplicar(s(2, "CREADO", 5));
        assertThat(vista.conteoPorEstado()).containsEntry("PAGADO", 1L).containsEntry("CREADO", 1L);
    }

    @Test
    void ignoraUnSnapshotMasAntiguoAlReprocesar() {
        vista.aplicar(s(1, "ENVIADO", 20));
        vista.aplicar(s(1, "CREADO", 0));
        assertThat(vista.listar()).singleElement().extracting(EstadoPedidoSnapshot::estado).isEqualTo("ENVIADO");
    }

    @Test
    void tombstoneEliminaElPedido() {
        vista.aplicar(s(1, "CREADO", 0));
        vista.eliminar(1L);
        assertThat(vista.vista().pedidos()).isZero();
        assertThat(vista.vista().registrosLeidos()).isEqualTo(2);
    }
}
