package cl.duoc.pedidos.kafkaadmin.admin.dto;

public record TopicoResumen(String nombre, int particiones, int replicas, boolean protegido, boolean interno,
                            String cleanupPolicy, String retentionMs, String minInsyncReplicas, long mensajes,
                            int particionesSubReplicadas) {
}
