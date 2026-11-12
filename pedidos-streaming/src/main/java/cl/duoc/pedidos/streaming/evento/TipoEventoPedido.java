package cl.duoc.pedidos.streaming.evento;

/** Tipos de evento de negocio que viajan por el topic pedidos.eventos. */
public enum TipoEventoPedido {
    PEDIDO_CREADO,
    ESTADO_CAMBIADO,
    BOLETA_SOLICITADA
}
