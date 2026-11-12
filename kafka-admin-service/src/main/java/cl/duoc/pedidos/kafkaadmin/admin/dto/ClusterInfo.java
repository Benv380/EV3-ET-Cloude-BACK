package cl.duoc.pedidos.kafkaadmin.admin.dto;

import java.util.List;

public record ClusterInfo(String clusterId, Integer controlador, int brokersEsperados, List<BrokerInfo> brokers) {
}
