package cl.duoc.pedidos.kafkaadmin.admin.dto;

public record BrokerInfo(int id, String host, int puerto, String rack, boolean controlador) {
}
