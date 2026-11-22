package cl.duoc.pedidos.pedidos.pedido;

import java.util.Optional;

/**
 * Puerto del dominio: ¿el usuario está bloqueado por posible fraude?
 * La implementación se alimenta del topic Kafka pedidos.bloqueos (paquete "mensajeria").
 */
public interface VerificadorBloqueos {

    Optional<BloqueoVigente> bloqueoVigente(String usuario);
}
