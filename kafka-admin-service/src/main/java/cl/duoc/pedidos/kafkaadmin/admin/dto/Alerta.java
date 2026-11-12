package cl.duoc.pedidos.kafkaadmin.admin.dto;

/**
 * Hallazgo del análisis de salud de Kafka.
 *
 * @param recurso broker, topic o consumer group afectado
 * @param mensaje qué está pasando
 * @param impacto efecto en disponibilidad/rendimiento y qué hacer
 */
public record Alerta(Nivel nivel, String recurso, String mensaje, String impacto) {

    public enum Nivel { CRITICO, ADVERTENCIA, INFO }
}
