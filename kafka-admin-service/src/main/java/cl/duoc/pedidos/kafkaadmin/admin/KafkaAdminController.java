package cl.duoc.pedidos.kafkaadmin.admin;

import cl.duoc.pedidos.common.web.RecursoNoEncontradoException;
import cl.duoc.pedidos.kafkaadmin.admin.dto.*;
import cl.duoc.pedidos.streaming.config.StreamingProperties;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.time.Clock;
import java.util.List;

/**
 * API de administración de Kafka (protegida por JWT; si ROL_ADMIN está definido, exige ese rol).
 * <pre>
 *  GET    /api/admin/kafka/cluster                      brokers y controlador
 *  GET    /api/admin/kafka/salud                        alertas: brokers caídos, sub-replicación, lag
 *  GET    /api/admin/kafka/topics                       listar topics con particiones, réplicas, retención
 *  GET    /api/admin/kafka/topics/{t}                   detalle: líder/ISR/offsets por partición y configs
 *  POST   /api/admin/kafka/topics                       crear (particiones, réplicas, retención, limpieza)
 *  PATCH  /api/admin/kafka/topics/{t}/configs           modificar o restablecer configuraciones
 *  POST   /api/admin/kafka/topics/{t}/particiones       aumentar particiones
 *  DELETE /api/admin/kafka/topics/{t}                   eliminar (los del sistema están protegidos)
 *  POST   /api/admin/kafka/topics/{t}/mensajes          publicar un mensaje de prueba
 *  GET    /api/admin/kafka/grupos                       consumer groups con estado, miembros y lag
 *  GET    /api/admin/kafka/grupos/{g}                   miembros, asignación de particiones, offsets y lag
 *  POST   /api/admin/kafka/grupos/{g}/offsets           mover offsets al inicio o al final
 *  DELETE /api/admin/kafka/grupos/{g}                   eliminar un grupo vacío
 *  GET    /api/admin/kafka/plantillas                   mensajes de negocio para probar (también para Kafka UI)
 *  POST   /api/admin/kafka/plantillas/{id}/publicar     publicar una plantilla
 * </pre>
 */
@RestController
@RequestMapping("/api/admin/kafka")
public class KafkaAdminController {

    private final KafkaAdminService servicio;
    private final PlantillasNegocio plantillas;

    public KafkaAdminController(KafkaAdminService servicio, StreamingProperties streaming) {
        this.servicio = servicio;
        this.plantillas = new PlantillasNegocio(streaming, Clock.systemUTC());
    }

    @GetMapping("/cluster")
    public ClusterInfo cluster() {
        return servicio.cluster();
    }

    @GetMapping("/salud")
    public SaludKafka salud() {
        return servicio.salud();
    }

    @GetMapping("/topics")
    public List<TopicoResumen> topics() {
        return servicio.topics();
    }

    @GetMapping("/topics/{nombre}")
    public TopicoDetalle topic(@PathVariable String nombre) {
        return servicio.topic(nombre);
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public TopicoDetalle crear(@Valid @RequestBody CrearTopicoRequest req) {
        return servicio.crearTopic(req);
    }

    @PatchMapping("/topics/{nombre}/configs")
    public TopicoDetalle configs(@PathVariable String nombre, @RequestBody ActualizarConfigRequest req) {
        return servicio.actualizarConfigs(nombre, req);
    }

    @PostMapping("/topics/{nombre}/particiones")
    public TopicoDetalle particiones(@PathVariable String nombre, @Valid @RequestBody AumentarParticionesRequest req) {
        return servicio.aumentarParticiones(nombre, req.total());
    }

    @DeleteMapping("/topics/{nombre}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void eliminar(@PathVariable String nombre) {
        servicio.eliminarTopic(nombre);
    }

    @PostMapping("/topics/{nombre}/mensajes")
    public MensajePublicado publicar(@PathVariable String nombre, @Valid @RequestBody MensajePruebaRequest req) {
        return servicio.publicar(nombre, req);
    }

    @GetMapping("/grupos")
    public List<GrupoResumen> grupos() {
        return servicio.grupos();
    }

    @GetMapping("/grupos/{id}")
    public GrupoDetalle grupo(@PathVariable String id) {
        return servicio.grupo(id);
    }

    @PostMapping("/grupos/{id}/offsets")
    public GrupoDetalle resetear(@PathVariable String id, @Valid @RequestBody ResetOffsetsRequest req) {
        return servicio.resetearOffsets(id, req);
    }

    @DeleteMapping("/grupos/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void eliminarGrupo(@PathVariable String id) {
        servicio.eliminarGrupo(id);
    }

    @GetMapping("/plantillas")
    public List<PlantillaMensaje> plantillas() {
        return plantillas.todas();
    }

    @PostMapping("/plantillas/{id}/publicar")
    public MensajePublicado publicarPlantilla(@PathVariable String id) {
        PlantillaMensaje p = plantillas.buscar(id)
                .orElseThrow(() -> new RecursoNoEncontradoException("No existe la plantilla " + id));
        return servicio.publicar(p.topico(), new MensajePruebaRequest(p.clave(), p.valor(),
                java.util.Map.of("tipo-evento", p.id(), "origen", "kafka-admin")));
    }
}
