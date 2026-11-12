package cl.duoc.pedidos.kafkaadmin.admin.dto;

import java.util.List;

public record GrupoResumen(String grupo, String estado, int miembros, long lagTotal, List<String> topicos) {
}
