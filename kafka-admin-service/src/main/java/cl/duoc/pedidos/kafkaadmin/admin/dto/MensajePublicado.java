package cl.duoc.pedidos.kafkaadmin.admin.dto;

public record MensajePublicado(String topico, int particion, long offset, String clave) {
}
