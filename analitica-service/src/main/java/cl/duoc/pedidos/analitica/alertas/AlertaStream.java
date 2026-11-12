package cl.duoc.pedidos.analitica.alertas;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Alerta generada en tiempo real por el grupo analitica-alertas. */
@Entity
@Table(name = "analitica_alertas")
public class AlertaStream {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "evento_id", nullable = false, unique = true)
    private UUID eventoId;

    @Column(nullable = false, length = 30)
    private String tipo;

    @Column(name = "pedido_id", nullable = false)
    private Long pedidoId;

    @Column(length = 120)
    private String cliente;

    @Column(precision = 14, scale = 2)
    private BigDecimal total;

    @Column(length = 300)
    private String detalle;

    private int particion;

    @Column(name = "offset_kafka")
    private long offset;

    @Column(name = "fecha_evento")
    private Instant fechaEvento;

    @Column(name = "detectada_en", nullable = false)
    private Instant detectadaEn;

    protected AlertaStream() {
    }

    public AlertaStream(UUID eventoId, TipoAlerta tipo, Long pedidoId, String cliente, BigDecimal total,
                        String detalle, int particion, long offset, Instant fechaEvento) {
        this.eventoId = eventoId;
        this.tipo = tipo.name();
        this.pedidoId = pedidoId;
        this.cliente = cliente;
        this.total = total;
        this.detalle = detalle;
        this.particion = particion;
        this.offset = offset;
        this.fechaEvento = fechaEvento;
        this.detectadaEn = Instant.now();
    }

    public Long getId() { return id; }
    public UUID getEventoId() { return eventoId; }
    public String getTipo() { return tipo; }
    public Long getPedidoId() { return pedidoId; }
    public String getCliente() { return cliente; }
    public BigDecimal getTotal() { return total; }
    public String getDetalle() { return detalle; }
    public int getParticion() { return particion; }
    public long getOffset() { return offset; }
    public Instant getFechaEvento() { return fechaEvento; }
    public Instant getDetectadaEn() { return detectadaEn; }
}
