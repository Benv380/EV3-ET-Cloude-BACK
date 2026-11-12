package cl.duoc.pedidos.kafkaadmin.admin.dto;

import java.util.List;

public record GrupoDetalle(String grupo, String estado, Integer coordinador, String asignador,
                           List<MiembroGrupo> miembros, List<OffsetParticion> offsets, long lagTotal) {
}
