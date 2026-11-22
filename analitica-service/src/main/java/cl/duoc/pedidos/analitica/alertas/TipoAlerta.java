package cl.duoc.pedidos.analitica.alertas;

public enum TipoAlerta {
    /** Pedido creado con un total igual o superior al umbral configurado. */
    ALTO_MONTO,
    /** Un pedido fue cancelado. */
    CANCELACION,
    /** Posible fraude: el usuario fue bloqueado (ráfaga de pedidos o monto excesivo). */
    FRAUDE
}
