package cl.duoc.pedidos.streaming.validacion;

import cl.duoc.pedidos.streaming.error.EventoInvalidoException;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.evento.TipoEventoPedido;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidadorEventosTest {

    private final ValidadorEventos validador =
            new ValidadorEventos(Validation.buildDefaultValidatorFactory().getValidator());

    static EventoPedidoStream evento(Long pedidoId, String cliente, BigDecimal total) {
        return new EventoPedidoStream(UUID.randomUUID(), 1, TipoEventoPedido.PEDIDO_CREADO, pedidoId, cliente, total,
                "CREADO", null, "ana", "test", "NINGUNA", Instant.now());
    }

    @Test
    void eventoCompletoEsValido() {
        EventoPedidoStream e = evento(1L, "Ana", new BigDecimal("1000"));
        assertThat(validador.validar(e)).isSameAs(e);
    }

    @Test
    void detallaTodosLosCamposInvalidos() {
        assertThatThrownBy(() -> validador.validar(evento(-5L, "", new BigDecimal("-1"))))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("cliente")
                .hasMessageContaining("pedidoId")
                .hasMessageContaining("total");
    }

    @Test
    void payloadNuloEsInvalido() {
        assertThatThrownBy(() -> validador.validar(null)).isInstanceOf(EventoInvalidoException.class);
    }
}
