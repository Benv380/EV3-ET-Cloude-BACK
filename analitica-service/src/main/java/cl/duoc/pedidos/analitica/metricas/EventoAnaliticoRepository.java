package cl.duoc.pedidos.analitica.metricas;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface EventoAnaliticoRepository extends JpaRepository<EventoAnalitico, Long> {

    boolean existsByEventoId(UUID eventoId);

    List<EventoAnalitico> findTop25ByOrderByIdDesc();

    List<EventoAnalitico> findByFechaEventoAfter(Instant desde);

    @Query("select e.tipo, count(e) from EventoAnalitico e group by e.tipo")
    List<Object[]> contarPorTipo();

    @Query("select e.particion, count(e) from EventoAnalitico e group by e.particion order by e.particion")
    List<Object[]> contarPorParticion();

    @Query("select e.consumidor, count(e) from EventoAnalitico e group by e.consumidor order by e.consumidor")
    List<Object[]> contarPorConsumidor();

    @Query("select coalesce(sum(e.total), 0) from EventoAnalitico e where e.tipo = 'PEDIDO_CREADO'")
    BigDecimal montoCreado();
}
