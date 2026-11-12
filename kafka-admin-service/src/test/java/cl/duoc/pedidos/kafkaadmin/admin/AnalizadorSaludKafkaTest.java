package cl.duoc.pedidos.kafkaadmin.admin;

import cl.duoc.pedidos.kafkaadmin.admin.dto.Alerta;
import cl.duoc.pedidos.kafkaadmin.admin.dto.GrupoResumen;
import cl.duoc.pedidos.kafkaadmin.admin.dto.SaludKafka;
import cl.duoc.pedidos.kafkaadmin.admin.dto.TopicoResumen;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AnalizadorSaludKafkaTest {

    private final AnalizadorSaludKafka analizador = new AnalizadorSaludKafka(3, 100, 2);

    private static TopicoResumen topic(String nombre, long mensajes, int subReplicadas) {
        return new TopicoResumen(nombre, 3, 3, true, false, "delete", "604800000", "2", mensajes, subReplicadas);
    }

    @Test
    void todoSanoEsOk() {
        SaludKafka s = analizador.analizar(3, List.of(topic("pedidos.eventos", 10, 0)),
                List.of(new GrupoResumen("analitica-eventos", "STABLE", 3, 0, List.of("pedidos.eventos"))), 0, "pedidos.dlt");
        assertThat(s.estado()).isEqualTo("OK");
        assertThat(s.alertas()).isEmpty();
    }

    @Test
    void unBrokerCaidoEsAdvertenciaYDosEsCritico() {
        assertThat(analizador.analizar(2, List.of(), List.of(), 0, "pedidos.dlt").estado()).isEqualTo("ADVERTENCIA");
        assertThat(analizador.analizar(1, List.of(), List.of(), 0, "pedidos.dlt").estado()).isEqualTo("CRITICO");
    }

    @Test
    void particionesSubReplicadasYSinLider() {
        SaludKafka s = analizador.analizar(3, List.of(topic("pedidos.eventos", 0, 2)), List.of(), 1, "pedidos.dlt");
        assertThat(s.particionesSubReplicadas()).isEqualTo(2);
        assertThat(s.estado()).isEqualTo("CRITICO");
    }

    @Test
    void grupoSinConsumidoresConPendientes() {
        SaludKafka s = analizador.analizar(3, List.of(),
                List.of(new GrupoResumen("analitica-alertas", "EMPTY", 0, 7, List.of("pedidos.eventos"))), 0, "pedidos.dlt");
        assertThat(s.alertas()).extracting(Alerta::mensaje).anyMatch(m -> m.contains("sin consumidores"));
        assertThat(s.lagPorGrupo()).containsEntry("analitica-alertas", 7L);
    }

    @Test
    void lagAltoYMensajesEnDlt() {
        SaludKafka s = analizador.analizar(3, List.of(topic("pedidos.dlt", 4, 0)),
                List.of(new GrupoResumen("analitica-eventos", "STABLE", 3, 500, List.of("pedidos.eventos"))), 0, "pedidos.dlt");
        assertThat(s.mensajesDlt()).isEqualTo(4);
        assertThat(s.alertas()).extracting(Alerta::nivel).contains(Alerta.Nivel.ADVERTENCIA, Alerta.Nivel.INFO);
    }
}
