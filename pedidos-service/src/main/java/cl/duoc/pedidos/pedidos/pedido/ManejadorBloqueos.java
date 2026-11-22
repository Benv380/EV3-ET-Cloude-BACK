package cl.duoc.pedidos.pedidos.pedido;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Respuesta HTTP 403 con el detalle del bloqueo (regla, motivo y hasta cuándo). */
@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ManejadorBloqueos {

    private static final Logger log = LoggerFactory.getLogger(ManejadorBloqueos.class);

    @ExceptionHandler(UsuarioBloqueadoException.class)
    public ProblemDetail bloqueado(UsuarioBloqueadoException e) {
        BloqueoVigente b = e.getBloqueo();
        log.warn("[BLOQUEO] pedido RECHAZADO usuario={} regla={} hasta={}", b.usuario(), b.regla(), b.hasta());
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.FORBIDDEN, e.getMessage());
        pd.setTitle("Usuario bloqueado por posible fraude");
        pd.setProperty("regla", b.regla());
        pd.setProperty("motivo", b.motivo());
        pd.setProperty("bloqueadoHasta", b.hasta().toString());
        return pd;
    }
}
