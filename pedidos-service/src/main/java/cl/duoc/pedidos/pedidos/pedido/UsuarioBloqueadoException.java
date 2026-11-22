package cl.duoc.pedidos.pedidos.pedido;

/** El usuario intentó crear un pedido mientras está bloqueado por posible fraude (HTTP 403). */
public class UsuarioBloqueadoException extends RuntimeException {

    private final transient BloqueoVigente bloqueo;

    public UsuarioBloqueadoException(BloqueoVigente bloqueo) {
        super("Usuario bloqueado por posible fraude (" + bloqueo.regla() + "): " + bloqueo.motivo()
                + ". Bloqueado hasta " + bloqueo.hasta());
        this.bloqueo = bloqueo;
    }

    public BloqueoVigente getBloqueo() {
        return bloqueo;
    }
}
