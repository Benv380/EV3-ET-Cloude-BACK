package cl.duoc.pedidos.streaming.error;

/**
 * Error NO recuperable: el payload no cumple el contrato (campos faltantes o inválidos).
 * Reintentar no lo arregla, así que el manejador de errores lo envía directo al DLT.
 */
public class EventoInvalidoException extends RuntimeException {

    public EventoInvalidoException(String mensaje) {
        super(mensaje);
    }
}
