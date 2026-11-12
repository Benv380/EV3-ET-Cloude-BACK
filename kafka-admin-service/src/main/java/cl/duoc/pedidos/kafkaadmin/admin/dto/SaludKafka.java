package cl.duoc.pedidos.kafkaadmin.admin.dto;

import java.util.List;
import java.util.Map;

public record SaludKafka(String estado, int brokersActivos, int brokersEsperados, int particionesSubReplicadas,
                         int particionesSinLider, Map<String, Long> lagPorGrupo, long mensajesDlt,
                         List<Alerta> alertas) {
}
