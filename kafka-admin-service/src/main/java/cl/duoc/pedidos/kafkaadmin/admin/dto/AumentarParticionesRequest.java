package cl.duoc.pedidos.kafkaadmin.admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public record AumentarParticionesRequest(@Min(1) @Max(100) int total) {
}
