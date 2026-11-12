package cl.duoc.pedidos.kafkaadmin.admin.dto;

import java.util.List;

public record TopicoDetalle(String nombre, boolean protegido, boolean interno, int replicas,
                            List<ParticionInfo> particiones, List<ConfigInfo> configs, List<String> grupos) {
}
