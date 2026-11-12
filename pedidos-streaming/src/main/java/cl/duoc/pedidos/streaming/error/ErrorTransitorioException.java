package cl.duoc.pedidos.streaming.error;

/**
 * Error RECUPERABLE (por ejemplo, una dependencia que no responde por un momento).
 * El manejador de errores reintenta con espera exponencial y, si se agotan los intentos, lo envía al DLT.
 */
public class ErrorTransitorioException extends RuntimeException {

    public ErrorTransitorioException(String mensaje) {
        super(mensaje);
    }
}
