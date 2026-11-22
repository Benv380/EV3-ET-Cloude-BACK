package cl.duoc.pedidos.analitica.web;

import cl.duoc.pedidos.analitica.fraude.FraudeService;
import cl.duoc.pedidos.analitica.fraude.VistaBloqueosService;
import cl.duoc.pedidos.streaming.evento.BloqueoUsuario;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Bloqueos por posible fraude: listar y desbloquear manualmente. */
@RestController
@RequestMapping("/api/analitica/bloqueos")
public class BloqueosController {

    private final VistaBloqueosService vista;
    private final FraudeService fraude;

    public BloqueosController(VistaBloqueosService vista, FraudeService fraude) {
        this.vista = vista;
        this.fraude = fraude;
    }

    @GetMapping
    public List<VistaBloqueosService.EstadoBloqueo> listar() {
        return vista.listar();
    }

    @PostMapping("/{usuario}/desbloquear")
    public BloqueoUsuario desbloquear(@PathVariable String usuario) {
        return fraude.desbloquear(usuario);
    }
}
