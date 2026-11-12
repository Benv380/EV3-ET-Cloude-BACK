package cl.duoc.pedidos.kafkaadmin.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * @param estrategia INICIO (reprocesar todo lo retenido) o FINAL (saltar lo pendiente)
 */
public record ResetOffsetsRequest(@NotBlank String topico, @NotNull Estrategia estrategia) {

    public enum Estrategia { INICIO, FINAL }
}
