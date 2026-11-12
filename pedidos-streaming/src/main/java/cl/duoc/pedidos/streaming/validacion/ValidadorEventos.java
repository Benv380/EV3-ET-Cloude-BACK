package cl.duoc.pedidos.streaming.validacion;

import cl.duoc.pedidos.streaming.error.EventoInvalidoException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Valida un DTO de Kafka con sus anotaciones (Bean Validation).
 * Se usa en el productor (antes de enviar) y en los consumidores (antes de aplicar la lógica de negocio).
 */
public class ValidadorEventos {

    private final Validator validator;

    public ValidadorEventos(Validator validator) {
        this.validator = validator;
    }

    public <T> T validar(T evento) {
        if (evento == null) {
            throw new EventoInvalidoException("payload vacío");
        }
        Set<ConstraintViolation<T>> errores = validator.validate(evento);
        if (!errores.isEmpty()) {
            String detalle = errores.stream()
                    .sorted(Comparator.comparing(v -> v.getPropertyPath().toString()))
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .collect(Collectors.joining(", "));
            throw new EventoInvalidoException(evento.getClass().getSimpleName() + " inválido: " + detalle);
        }
        return evento;
    }
}
