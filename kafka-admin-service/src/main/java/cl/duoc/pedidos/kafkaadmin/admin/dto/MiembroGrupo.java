package cl.duoc.pedidos.kafkaadmin.admin.dto;

import java.util.List;

public record MiembroGrupo(String consumidor, String clientId, String host, List<String> particiones) {
}
