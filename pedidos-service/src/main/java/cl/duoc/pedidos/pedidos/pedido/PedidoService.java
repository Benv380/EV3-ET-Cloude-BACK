package cl.duoc.pedidos.pedidos.pedido;

import cl.duoc.pedidos.common.mensajeria.evento.PedidoEvento;
import cl.duoc.pedidos.common.mensajeria.evento.SimulacionFallo;
import cl.duoc.pedidos.common.mensajeria.evento.TipoEvento;
import cl.duoc.pedidos.common.seguridad.UsuarioActual;
import cl.duoc.pedidos.common.web.RecursoNoEncontradoException;
import cl.duoc.pedidos.common.web.ReglaNegocioException;
import cl.duoc.pedidos.pedidos.pedido.dto.CambiarEstadoRequest;
import cl.duoc.pedidos.pedidos.pedido.dto.CrearPedidoRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Lógica de negocio de pedidos. No conoce RabbitMQ ni Kafka: solo usa la interfaz {@link EventosPedidoPublisher}.
 * Spring inyecta TODAS las implementaciones (RabbitMQ y Kafka) y cada evento se entrega a cada una.
 * Las operaciones son transaccionales: si la publicación en RabbitMQ falla, el cambio en BD se revierte.
 */
@Service
public class PedidoService {

    private final PedidoRepository repositorio;
    private final List<EventosPedidoPublisher> publicadores;
    private final VerificadorBloqueos bloqueos;

    public PedidoService(PedidoRepository repositorio, List<EventosPedidoPublisher> publicadores,
                         VerificadorBloqueos bloqueos) {
        this.repositorio = repositorio;
        this.publicadores = List.copyOf(publicadores);
        this.bloqueos = bloqueos;
    }

    @Transactional(readOnly = true)
    public List<Pedido> listar() {
        return repositorio.findAllByOrderByIdDesc();
    }

    @Transactional(readOnly = true)
    public Pedido obtener(Long id) {
        return repositorio.findById(id)
                .orElseThrow(() -> new RecursoNoEncontradoException("No existe el pedido " + id));
    }

    @Transactional
    public Pedido crear(CrearPedidoRequest solicitud) {
        // Bloqueo por posible fraude decidido por analitica-service y recibido por Kafka (pedidos.bloqueos).
        bloqueos.bloqueoVigente(UsuarioActual.nombre()).ifPresent(b -> {
            throw new UsuarioBloqueadoException(b);
        });
        Pedido pedido = repositorio.save(new Pedido(solicitud.cliente().trim(), solicitud.emailCliente().trim(),
                solicitud.descripcion().trim(), solicitud.total(), UsuarioActual.nombre()));
        PedidoEvento evento = evento(TipoEvento.PEDIDO_CREADO, pedido, null, solicitud.simularFallo());
        publicadores.forEach(p -> p.publicarPedidoCreado(evento));
        return pedido;
    }

    @Transactional
    public Pedido cambiarEstado(Long id, CambiarEstadoRequest solicitud) {
        Pedido pedido = obtener(id);
        EstadoPedido anterior = pedido.getEstado();
        EstadoPedido nuevo = solicitud.estado();
        if (!anterior.puedeCambiarA(nuevo)) {
            throw new ReglaNegocioException("No se puede pasar de " + anterior + " a " + nuevo
                    + ". Permitidos: " + anterior.siguientesPermitidos());
        }
        pedido.cambiarEstado(nuevo);
        repositorio.save(pedido);
        PedidoEvento evento = evento(TipoEvento.ESTADO_CAMBIADO, pedido, anterior, solicitud.simularFallo());
        publicadores.forEach(p -> p.publicarCambioEstado(evento));
        return pedido;
    }

    @Transactional(readOnly = true)
    public PedidoEvento solicitarBoleta(Long id) {
        Pedido pedido = obtener(id);
        if (pedido.getEstado() == EstadoPedido.CREADO || pedido.getEstado() == EstadoPedido.CANCELADO) {
            throw new ReglaNegocioException("Solo se emite boleta de pedidos pagados (estado actual: "
                    + pedido.getEstado() + ")");
        }
        PedidoEvento evento = evento(TipoEvento.BOLETA_SOLICITADA, pedido, null, SimulacionFallo.NINGUNA);
        publicadores.forEach(p -> p.solicitarBoleta(evento));
        return evento;
    }

    private static PedidoEvento evento(TipoEvento tipo, Pedido p, EstadoPedido anterior, SimulacionFallo fallo) {
        return new PedidoEvento(UUID.randomUUID(), tipo, p.getId(), p.getCliente(), p.getEmailCliente(),
                p.getDescripcion(), p.getTotal(), p.getEstado().name(), anterior != null ? anterior.name() : null,
                UsuarioActual.nombre(), fallo != null ? fallo : SimulacionFallo.NINGUNA, Instant.now());
    }
}
