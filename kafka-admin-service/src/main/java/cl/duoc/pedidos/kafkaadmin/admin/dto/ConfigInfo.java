package cl.duoc.pedidos.kafkaadmin.admin.dto;

/** @param porDefecto true si el valor es el del broker (no se configuró en el topic) */
public record ConfigInfo(String nombre, String valor, boolean porDefecto, String fuente) {
}
