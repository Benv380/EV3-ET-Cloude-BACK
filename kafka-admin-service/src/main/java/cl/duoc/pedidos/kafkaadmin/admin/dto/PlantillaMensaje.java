package cl.duoc.pedidos.kafkaadmin.admin.dto;

/**
 * Mensaje de negocio listo para publicar (desde esta API o pegándolo en Kafka UI → Produce Message).
 *
 * @param efectoEsperado qué debería pasar en el sistema al publicarlo
 */
public record PlantillaMensaje(String id, String titulo, String topico, String clave, String valor,
                               String efectoEsperado) {
}
