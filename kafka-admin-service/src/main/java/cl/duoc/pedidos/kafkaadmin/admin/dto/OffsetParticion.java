package cl.duoc.pedidos.kafkaadmin.admin.dto;

/**
 * @param offsetConfirmado último offset confirmado por el grupo (null = aún no confirma nada)
 * @param offsetFinal      offset del próximo mensaje que se escribirá (log-end-offset)
 * @param lag              mensajes pendientes de procesar = offsetFinal - offsetConfirmado
 */
public record OffsetParticion(String topico, int particion, Long offsetConfirmado, long offsetFinal, long lag,
                              String consumidor) {
}
