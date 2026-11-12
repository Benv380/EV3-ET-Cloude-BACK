package cl.duoc.pedidos.kafkaadmin.admin;

import cl.duoc.pedidos.common.web.SolicitudInvalidaException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidadorTopicosTest {

    private final ValidadorTopicos validador = new ValidadorTopicos(12);

    @Test
    void topicValidoConRetencionYCompactacion() {
        assertThatCode(() -> validador.validarCreacion("ventas.diarias", 3, 3, 3,
                Map.of("cleanup.policy", "compact,delete", "retention.ms", "86400000", "min.insync.replicas", "2")))
                .doesNotThrowAnyException();
    }

    @Test
    void rechazaMasReplicasQueBrokers() {
        assertThatThrownBy(() -> validador.validarCreacion("x", 3, 4, 3, Map.of()))
                .isInstanceOf(SolicitudInvalidaException.class).hasMessageContaining("brokers");
    }

    @Test
    void rechazaMinInsyncMayorQueReplicas() {
        assertThatThrownBy(() -> validador.validarCreacion("x", 3, 2, 3, Map.of("min.insync.replicas", "3")))
                .isInstanceOf(SolicitudInvalidaException.class).hasMessageContaining("min.insync.replicas");
    }

    @Test
    void rechazaNombresReservadosOInvalidos() {
        assertThatThrownBy(() -> validador.validarNombre("__consumer_offsets")).isInstanceOf(SolicitudInvalidaException.class);
        assertThatThrownBy(() -> validador.validarNombre("con espacios")).isInstanceOf(SolicitudInvalidaException.class);
        assertThatThrownBy(() -> validador.validarNombre("..")).isInstanceOf(SolicitudInvalidaException.class);
    }

    @Test
    void rechazaPoliticaDeLimpiezaDesconocida() {
        assertThatThrownBy(() -> validador.validarConfig("cleanup.policy", "borrar", 3))
                .isInstanceOf(SolicitudInvalidaException.class);
    }

    @Test
    void rechazaRetencionMenorAUnMinutoPeroAceptaIlimitada() {
        assertThatThrownBy(() -> validador.validarConfig("retention.ms", "500", 3))
                .isInstanceOf(SolicitudInvalidaException.class);
        assertThatCode(() -> validador.validarConfig("retention.ms", "-1", 3)).doesNotThrowAnyException();
    }

    @Test
    void rechazaConfiguracionNoPermitida() {
        assertThatThrownBy(() -> validador.validarConfig("unclean.leader.election.enable", "true", 3))
                .isInstanceOf(SolicitudInvalidaException.class).hasMessageContaining("no permitida");
    }

    @Test
    void soloPermiteAumentarParticiones() {
        assertThatThrownBy(() -> validador.validarAumentoParticiones(3, 2)).isInstanceOf(SolicitudInvalidaException.class);
        assertThatThrownBy(() -> validador.validarAumentoParticiones(3, 3)).isInstanceOf(SolicitudInvalidaException.class);
        assertThatCode(() -> validador.validarAumentoParticiones(3, 6)).doesNotThrowAnyException();
    }

    @Test
    void rechazaValoresNoNumericos() {
        assertThatThrownBy(() -> validador.validarConfig("retention.ms", "siete dias", 3))
                .isInstanceOf(SolicitudInvalidaException.class).hasMessageContaining("entero");
    }
}
