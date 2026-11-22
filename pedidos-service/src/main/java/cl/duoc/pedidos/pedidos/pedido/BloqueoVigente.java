package cl.duoc.pedidos.pedidos.pedido;

import java.time.Instant;

/** Bloqueo por posible fraude vigente para un usuario (modelo de dominio, sin tipos de Kafka). */
public record BloqueoVigente(String usuario, String regla, String motivo, Instant hasta) {
}
