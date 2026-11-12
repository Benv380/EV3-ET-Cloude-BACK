package cl.duoc.pedidos.analitica.metricas;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Cada evento de pedidos.eventos procesado por el grupo analitica-eventos, con su posición en Kafka. */
@Entity
@Table(name = "analitica_eventos", indexes = @Index(name = "ix_analitica_fecha", columnList = "fecha_evento"))
public class EventoAnalitico {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "evento_id", nullable = false, unique = true)
    private UUID eventoId;

    @Column(nullable = false, length = 40)
    private String tipo;

    @Column(name = "pedido_id", nullable = false)
    private Long pedidoId;

    @Column(length = 120)
    private String cliente;

    @Column(precision = 14, scale = 2)
    private BigDecimal total;

    @Column(length = 20)
    private String estado;

    @Column(name = "estado_anterior", length = 20)
    private String estadoAnterior;

    @Column(length = 80)
    private String topico;

    private int particion;

    @Column(name = "offset_kafka")
    private long offset;

    @Column(length = 60)
    private String consumidor;

    @Column(name = "fecha_evento", nullable = false)
    private Instant fechaEvento;

    @Column(name = "procesado_en", nullable = false)
    private Instant procesadoEn;

    @Column(name = "latencia_ms")
    private long latenciaMs;

    protected EventoAnalitico() {
    }

    public EventoAnalitico(UUID eventoId, String tipo, Long pedidoId, String cliente, BigDecimal total, String estado,
                           String estadoAnterior, String topico, int particion, long offset, String consumidor,
                           Instant fechaEvento, Instant procesadoEn) {
        this.eventoId = eventoId;
        this.tipo = tipo;
        this.pedidoId = pedidoId;
        this.cliente = cliente;
        this.total = total;
        this.estado = estado;
        this.estadoAnterior = estadoAnterior;
        this.topico = topico;
        this.particion = particion;
        this.offset = offset;
        this.consumidor = consumidor;
        this.fechaEvento = fechaEvento;
        this.procesadoEn = procesadoEn;
        this.latenciaMs = Math.max(0, procesadoEn.toEpochMilli() - fechaEvento.toEpochMilli());
    }

    public Long getId() { return id; }
    public UUID getEventoId() { return eventoId; }
    public String getTipo() { return tipo; }
    public Long getPedidoId() { return pedidoId; }
    public String getCliente() { return cliente; }
    public BigDecimal getTotal() { return total; }
    public String getEstado() { return estado; }
    public String getEstadoAnterior() { return estadoAnterior; }
    public String getTopico() { return topico; }
    public int getParticion() { return particion; }
    public long getOffset() { return offset; }
    public String getConsumidor() { return consumidor; }
    public Instant getFechaEvento() { return fechaEvento; }
    public Instant getProcesadoEn() { return procesadoEn; }
    public long getLatenciaMs() { return latenciaMs; }
}
