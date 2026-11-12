package cl.duoc.pedidos.kafkaadmin.admin.dto;

import jakarta.validation.constraints.NotNull;

import java.util.Map;

/** Mensaje de texto libre (JSON válido o no) para probar consumidores, validación y DLT. */
public record MensajePruebaRequest(String clave, @NotNull String valor, Map<String, String> headers) {

    public MensajePruebaRequest {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
