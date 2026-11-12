package cl.duoc.pedidos.analitica.consumo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetadatosConsumoTest {

    @Test
    void identificaAlConsumidorDentroDelGrupo() {
        assertThat(MetadatosConsumo.nombreConsumidor("analitica-eventos-2-C-1")).isEqualTo("consumidor-2");
        assertThat(MetadatosConsumo.nombreConsumidor("main")).isEqualTo("main");
    }
}
