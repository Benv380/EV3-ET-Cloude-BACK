package cl.duoc.pedidos.kafkaadmin.admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;

/**
 * @param configs configuraciones del topic, por ejemplo {"cleanup.policy":"delete","retention.ms":"86400000"}
 */
public record CrearTopicoRequest(@NotBlank String nombre, @Min(1) @Max(100) int particiones,
                                 @Min(1) @Max(9) short replicas, Map<String, String> configs) {

    public CrearTopicoRequest {
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }
}
