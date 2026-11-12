package cl.duoc.pedidos.analitica.alertas;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface AlertaStreamRepository extends JpaRepository<AlertaStream, Long> {

    boolean existsByEventoId(UUID eventoId);

    List<AlertaStream> findTop50ByOrderByIdDesc();
}
