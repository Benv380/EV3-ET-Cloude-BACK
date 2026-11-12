package cl.duoc.pedidos.analitica.web;

import cl.duoc.pedidos.analitica.alertas.AlertaStream;
import cl.duoc.pedidos.analitica.alertas.AlertasService;
import cl.duoc.pedidos.analitica.config.AnaliticaProperties;
import cl.duoc.pedidos.analitica.dlt.MensajeDlt;
import cl.duoc.pedidos.analitica.dlt.MonitorDltService;
import cl.duoc.pedidos.analitica.estados.ProyeccionEstadosService;
import cl.duoc.pedidos.analitica.metricas.MetricasService;
import cl.duoc.pedidos.analitica.metricas.ResumenAnalitica;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Datos del dashboard de streaming (el frontend los consulta cada 2 segundos). */
@RestController
@RequestMapping("/api/analitica")
public class AnaliticaController {

    private final MetricasService metricas;
    private final AlertasService alertas;
    private final ProyeccionEstadosService estados;
    private final MonitorDltService dlt;
    private final AnaliticaProperties props;

    public AnaliticaController(MetricasService metricas, AlertasService alertas, ProyeccionEstadosService estados,
                               MonitorDltService dlt, AnaliticaProperties props) {
        this.metricas = metricas;
        this.alertas = alertas;
        this.estados = estados;
        this.dlt = dlt;
        this.props = props;
    }

    @GetMapping("/resumen")
    public ResumenAnalitica resumen() {
        return metricas.resumen(estados.conteoPorEstado(), alertas.total(), dlt.total(), props.ventanaMinutos());
    }

    @GetMapping("/estados")
    public ProyeccionEstadosService.VistaEstados estados() {
        return estados.vista();
    }

    @GetMapping("/alertas")
    public List<AlertaStream> alertas() {
        return alertas.ultimas();
    }

    @GetMapping("/dlt")
    public List<MensajeDlt> dlt() {
        return dlt.ultimos();
    }
}
