package cl.duoc.pedidos.kafkaadmin.admin.dto;

import java.util.List;
import java.util.Map;

/**
 * @param establecer configs a fijar en el topic (SET)
 * @param restablecer configs a volver al valor por defecto del broker (DELETE)
 */
public record ActualizarConfigRequest(Map<String, String> establecer, List<String> restablecer) {

    public ActualizarConfigRequest {
        establecer = establecer == null ? Map.of() : Map.copyOf(establecer);
        restablecer = restablecer == null ? List.of() : List.copyOf(restablecer);
    }
}
