package cl.duoc.pedidos.kafkaadmin.admin;

import cl.duoc.pedidos.common.web.RecursoNoEncontradoException;
import cl.duoc.pedidos.common.web.ReglaNegocioException;
import cl.duoc.pedidos.common.web.SolicitudInvalidaException;
import cl.duoc.pedidos.kafkaadmin.admin.dto.*;
import cl.duoc.pedidos.kafkaadmin.config.KafkaAdminConfig;
import cl.duoc.pedidos.kafkaadmin.config.KafkaAdminProperties;
import cl.duoc.pedidos.streaming.config.StreamingProperties;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Operaciones de administración de Kafka con AdminClient: topics, configuraciones, particiones,
 * consumer groups (miembros, offsets y lag) y salud del clúster.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);
    /** Configs que se muestran siempre en el detalle de un topic, aunque tengan el valor por defecto. */
    private static final Set<String> CONFIGS_CLAVE = Set.of("cleanup.policy", "retention.ms", "retention.bytes",
            "min.insync.replicas", "segment.ms", "max.message.bytes", "compression.type", "min.cleanable.dirty.ratio",
            "delete.retention.ms", "unclean.leader.election.enable");

    private final AdminClient admin;
    private final KafkaTemplate<String, String> plantillaTexto;
    private final StreamingProperties streaming;
    private final KafkaAdminProperties props;
    private final ValidadorTopicos validador;
    private final AnalizadorSaludKafka analizador;

    public KafkaAdminService(AdminClient admin, KafkaAdminConfig.ProductorTexto productorTexto,
                             StreamingProperties streaming, KafkaAdminProperties props) {
        this.admin = admin;
        this.plantillaTexto = productorTexto.template();
        this.streaming = streaming;
        this.props = props;
        this.validador = new ValidadorTopicos(props.maxParticiones());
        this.analizador = new AnalizadorSaludKafka(props.brokersEsperados(), props.umbralLag(), 2);
    }

    // =============================== CLÚSTER ===============================

    public ClusterInfo cluster() {
        DescribeClusterResult r = admin.describeCluster();
        Node controlador = esperar(r.controller());
        List<BrokerInfo> brokers = esperar(r.nodes()).stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(n -> new BrokerInfo(n.id(), n.host(), n.port(), n.rack(),
                        controlador != null && controlador.id() == n.id()))
                .toList();
        return new ClusterInfo(esperar(r.clusterId()), controlador != null ? controlador.id() : null,
                props.brokersEsperados(), brokers);
    }

    public SaludKafka salud() {
        int brokers = esperar(admin.describeCluster().nodes()).size();
        Map<String, TopicDescription> descripciones = describir(nombresTopics(false));
        int sinLider = (int) descripciones.values().stream().flatMap(d -> d.partitions().stream())
                .filter(p -> p.leader() == null || p.leader().isEmpty()).count();
        return analizador.analizar(brokers, topics(), grupos(), sinLider, streaming.topicos().dlt().nombre());
    }

    // =============================== TOPICS ===============================

    public List<TopicoResumen> topics() {
        Map<String, TopicDescription> descripciones = describir(nombresTopics(false));
        Map<String, Config> configs = configs(descripciones.keySet());
        Map<TopicPartition, Long> inicio = offsets(particiones(descripciones.values()), OffsetSpec.earliest());
        Map<TopicPartition, Long> fin = offsets(particiones(descripciones.values()), OffsetSpec.latest());
        return descripciones.values().stream()
                .sorted(Comparator.comparing(TopicDescription::name))
                .map(d -> {
                    Config c = configs.get(d.name());
                    long mensajes = d.partitions().stream().mapToLong(p -> {
                        TopicPartition tp = new TopicPartition(d.name(), p.partition());
                        return fin.getOrDefault(tp, 0L) - inicio.getOrDefault(tp, 0L);
                    }).sum();
                    int sub = (int) d.partitions().stream().filter(p -> p.isr().size() < p.replicas().size()).count();
                    return new TopicoResumen(d.name(), d.partitions().size(), replicas(d), protegido(d.name()),
                            d.isInternal(), valor(c, "cleanup.policy"), valor(c, "retention.ms"),
                            valor(c, "min.insync.replicas"), mensajes, sub);
                })
                .toList();
    }

    public TopicoDetalle topic(String nombre) {
        TopicDescription d = describir(List.of(nombre)).get(nombre);
        List<TopicPartition> tps = particiones(List.of(d));
        Map<TopicPartition, Long> inicio = offsets(tps, OffsetSpec.earliest());
        Map<TopicPartition, Long> fin = offsets(tps, OffsetSpec.latest());
        List<ParticionInfo> particiones = d.partitions().stream().map(p -> {
            TopicPartition tp = new TopicPartition(nombre, p.partition());
            long i = inicio.getOrDefault(tp, 0L);
            long f = fin.getOrDefault(tp, 0L);
            return new ParticionInfo(p.partition(), p.leader() == null || p.leader().isEmpty() ? null : p.leader().id(),
                    ids(p.replicas()), ids(p.isr()), i, f, f - i, p.isr().size() < p.replicas().size());
        }).toList();
        Config c = configs(List.of(nombre)).get(nombre);
        List<ConfigInfo> configs = c.entries().stream()
                .filter(e -> CONFIGS_CLAVE.contains(e.name()) || e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                .sorted(Comparator.comparing(ConfigEntry::name))
                .map(e -> new ConfigInfo(e.name(), e.value(), e.source() != ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
                        e.source().name()))
                .toList();
        List<String> grupos = grupos().stream().filter(g -> g.topicos().contains(nombre)).map(GrupoResumen::grupo).toList();
        return new TopicoDetalle(nombre, protegido(nombre), d.isInternal(), replicas(d), particiones, configs, grupos);
    }

    public TopicoDetalle crearTopic(CrearTopicoRequest req) {
        int brokers = esperar(admin.describeCluster().nodes()).size();
        validador.validarCreacion(req.nombre(), req.particiones(), req.replicas(), brokers, req.configs());
        NewTopic nuevo = new NewTopic(req.nombre(), req.particiones(), req.replicas()).configs(req.configs());
        esperar(admin.createTopics(List.of(nuevo)).all());
        log.info("[KAFKA-ADMIN] topic creado {} particiones={} replicas={} configs={}", req.nombre(),
                req.particiones(), req.replicas(), req.configs());
        return topic(req.nombre());
    }

    public TopicoDetalle actualizarConfigs(String nombre, ActualizarConfigRequest req) {
        TopicDescription d = describir(List.of(nombre)).get(nombre);
        if (req.establecer().isEmpty() && req.restablecer().isEmpty()) {
            throw new SolicitudInvalidaException("Indique al menos una configuración a establecer o restablecer");
        }
        validador.validarConfigs(req.establecer(), replicas(d));
        req.restablecer().forEach(clave -> {
            if (!ValidadorTopicos.CONFIGS_PERMITIDAS.contains(clave)) {
                throw new SolicitudInvalidaException("Configuración no permitida: " + clave);
            }
        });
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        List<AlterConfigOp> ops = new ArrayList<>();
        req.establecer().forEach((k, v) -> ops.add(new AlterConfigOp(new ConfigEntry(k, v.trim()), AlterConfigOp.OpType.SET)));
        req.restablecer().forEach(k -> ops.add(new AlterConfigOp(new ConfigEntry(k, null), AlterConfigOp.OpType.DELETE)));
        esperar(admin.incrementalAlterConfigs(Map.of(recurso, ops)).all());
        log.info("[KAFKA-ADMIN] configs de {} actualizadas: set={} reset={}", nombre, req.establecer(), req.restablecer());
        return topic(nombre);
    }

    public TopicoDetalle aumentarParticiones(String nombre, int total) {
        TopicDescription d = describir(List.of(nombre)).get(nombre);
        validador.validarAumentoParticiones(d.partitions().size(), total);
        esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(total))).all());
        log.warn("[KAFKA-ADMIN] {} ahora tiene {} particiones. Las claves nuevas pueden caer en otra partición"
                + " (el orden por clave solo se garantiza dentro de una partición).", nombre, total);
        return topic(nombre);
    }

    public void eliminarTopic(String nombre) {
        validador.validarNombre(nombre);
        if (protegido(nombre)) {
            throw new ReglaNegocioException("El topic " + nombre + " es parte del sistema y no se puede eliminar");
        }
        describir(List.of(nombre));
        esperar(admin.deleteTopics(List.of(nombre)).all());
        log.warn("[KAFKA-ADMIN] topic eliminado {}", nombre);
    }

    public MensajePublicado publicar(String topico, MensajePruebaRequest req) {
        describir(List.of(topico));
        ProducerRecord<String, String> registro = new ProducerRecord<>(topico, req.clave(), req.valor());
        req.headers().forEach((k, v) -> registro.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
        RecordMetadata meta;
        try {
            meta = plantillaTexto.send(registro).get(props.timeoutMs(), TimeUnit.MILLISECONDS).getRecordMetadata();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Publicación interrumpida", e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("No se pudo publicar en " + topico + ": " + e.getMessage(), e);
        }
        log.info("[KAFKA-ADMIN] mensaje de prueba -> {}-{}@{} clave={}", meta.topic(), meta.partition(), meta.offset(),
                req.clave());
        return new MensajePublicado(meta.topic(), meta.partition(), meta.offset(), req.clave());
    }

    // =============================== CONSUMER GROUPS ===============================

    public List<GrupoResumen> grupos() {
        List<String> ids = esperar(admin.listConsumerGroups().all()).stream()
                .map(ConsumerGroupListing::groupId).sorted().toList();
        if (ids.isEmpty()) {
            return List.of();
        }
        Map<String, ConsumerGroupDescription> desc = esperar(admin.describeConsumerGroups(ids).all());
        List<GrupoResumen> resultado = new ArrayList<>();
        for (String id : ids) {
            List<OffsetParticion> offsets = offsetsGrupo(id, desc.get(id));
            Set<String> topicos = new TreeSet<>();
            offsets.forEach(o -> topicos.add(o.topico()));
            resultado.add(new GrupoResumen(id, desc.get(id).state().name(), desc.get(id).members().size(),
                    offsets.stream().mapToLong(OffsetParticion::lag).sum(), List.copyOf(topicos)));
        }
        return resultado;
    }

    public GrupoDetalle grupo(String id) {
        ConsumerGroupDescription d = esperar(admin.describeConsumerGroups(List.of(id)).all()).get(id);
        if (d == null || d.state() == ConsumerGroupState.DEAD) {
            throw new RecursoNoEncontradoException("No existe el consumer group " + id);
        }
        List<MiembroGrupo> miembros = d.members().stream()
                .map(m -> new MiembroGrupo(m.consumerId(), m.clientId(), m.host(),
                        m.assignment().topicPartitions().stream().map(tp -> tp.topic() + "-" + tp.partition())
                                .sorted().toList()))
                .sorted(Comparator.comparing(MiembroGrupo::clientId))
                .toList();
        List<OffsetParticion> offsets = offsetsGrupo(id, d);
        return new GrupoDetalle(id, d.state().name(), d.coordinator() != null ? d.coordinator().id() : null,
                d.partitionAssignor(), miembros, offsets, offsets.stream().mapToLong(OffsetParticion::lag).sum());
    }

    /** Mueve los offsets del grupo al inicio (reprocesar lo retenido) o al final (saltar lo pendiente). */
    public GrupoDetalle resetearOffsets(String id, ResetOffsetsRequest req) {
        GrupoDetalle actual = grupo(id);
        if (!actual.miembros().isEmpty()) {
            throw new ReglaNegocioException("El grupo " + id + " tiene " + actual.miembros().size()
                    + " consumidores activos. Kafka solo permite mover offsets de un grupo sin consumidores"
                    + " (detenga el servicio y vuelva a intentar).");
        }
        TopicDescription d = describir(List.of(req.topico())).get(req.topico());
        List<TopicPartition> tps = particiones(List.of(d));
        Map<TopicPartition, Long> destino = offsets(tps, req.estrategia() == ResetOffsetsRequest.Estrategia.INICIO
                ? OffsetSpec.earliest() : OffsetSpec.latest());
        Map<TopicPartition, OffsetAndMetadata> nuevos = destino.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
        esperar(admin.alterConsumerGroupOffsets(id, nuevos).all());
        log.warn("[KAFKA-ADMIN] offsets del grupo {} en {} movidos a {}", id, req.topico(), req.estrategia());
        return grupo(id);
    }

    public void eliminarGrupo(String id) {
        if (streaming.grupos().todos().contains(id)) {
            throw new ReglaNegocioException("El grupo " + id + " es parte del sistema y no se puede eliminar");
        }
        esperar(admin.deleteConsumerGroups(List.of(id)).all());
        log.warn("[KAFKA-ADMIN] consumer group eliminado {}", id);
    }

    // =============================== Auxiliares ===============================

    private List<OffsetParticion> offsetsGrupo(String id, ConsumerGroupDescription d) {
        Map<TopicPartition, OffsetAndMetadata> confirmados = esperar(
                admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata());
        Map<TopicPartition, String> dueño = new HashMap<>();
        if (d != null) {
            d.members().forEach(m -> m.assignment().topicPartitions().forEach(tp -> dueño.put(tp, m.clientId())));
        }
        Set<TopicPartition> todas = new HashSet<>(confirmados.keySet());
        todas.addAll(dueño.keySet());
        if (todas.isEmpty()) {
            return List.of();
        }
        List<TopicPartition> lista = new ArrayList<>(todas);
        Map<TopicPartition, Long> fin = offsets(lista, OffsetSpec.latest());
        Map<TopicPartition, Long> inicio = offsets(lista, OffsetSpec.earliest());
        return lista.stream()
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .map(tp -> {
                    OffsetAndMetadata om = confirmados.get(tp);
                    Long confirmado = om != null ? om.offset() : null;
                    long f = fin.getOrDefault(tp, 0L);
                    long base = confirmado != null ? confirmado : inicio.getOrDefault(tp, 0L);
                    return new OffsetParticion(tp.topic(), tp.partition(), confirmado, f, Math.max(0, f - base),
                            dueño.get(tp));
                })
                .toList();
    }

    private Set<String> nombresTopics(boolean internos) {
        return esperar(admin.listTopics(new ListTopicsOptions().listInternal(internos)).names());
    }

    private Map<String, TopicDescription> describir(Collection<String> nombres) {
        if (nombres.isEmpty()) {
            return Map.of();
        }
        return esperar(admin.describeTopics(nombres).allTopicNames());
    }

    private Map<String, Config> configs(Collection<String> nombres) {
        if (nombres.isEmpty()) {
            return Map.of();
        }
        List<ConfigResource> recursos = nombres.stream()
                .map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n)).toList();
        Map<ConfigResource, Config> r = esperar(admin.describeConfigs(recursos).all());
        Map<String, Config> porNombre = new HashMap<>();
        r.forEach((k, v) -> porNombre.put(k.name(), v));
        return porNombre;
    }

    private Map<TopicPartition, Long> offsets(Collection<TopicPartition> tps, OffsetSpec spec) {
        if (tps.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> pedido = new HashMap<>();
        tps.forEach(tp -> pedido.put(tp, spec));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> r = esperar(admin.listOffsets(pedido).all());
        Map<TopicPartition, Long> salida = new HashMap<>();
        r.forEach((tp, info) -> salida.put(tp, info.offset()));
        return salida;
    }

    private static List<TopicPartition> particiones(Collection<TopicDescription> descripciones) {
        List<TopicPartition> tps = new ArrayList<>();
        descripciones.forEach(d -> d.partitions().forEach(p -> tps.add(new TopicPartition(d.name(), p.partition()))));
        return tps;
    }

    private static int replicas(TopicDescription d) {
        return d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size();
    }

    private static List<Integer> ids(List<Node> nodos) {
        return nodos.stream().map(Node::id).toList();
    }

    private static String valor(Config c, String nombre) {
        if (c == null) {
            return null;
        }
        ConfigEntry e = c.get(nombre);
        return e == null ? null : e.value();
    }

    private boolean protegido(String nombre) {
        return streaming.topicos().esProtegido(nombre) || nombre.startsWith("__");
    }

    /** Espera el resultado del AdminClient y traduce los errores de Kafka a respuestas HTTP claras. */
    private <T> T esperar(KafkaFuture<T> futuro) {
        try {
            return futuro.get(props.timeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operación interrumpida", e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Kafka no respondió en " + props.timeoutMs() + " ms", e);
        } catch (ExecutionException e) {
            Throwable causa = e.getCause();
            if (causa instanceof UnknownTopicOrPartitionException) {
                throw new RecursoNoEncontradoException("El topic no existe");
            }
            if (causa instanceof GroupIdNotFoundException) {
                throw new RecursoNoEncontradoException("El consumer group no existe");
            }
            if (causa instanceof TopicExistsException) {
                throw new ReglaNegocioException("El topic ya existe");
            }
            if (causa instanceof GroupNotEmptyException || causa instanceof UnknownMemberIdException) {
                throw new ReglaNegocioException("El consumer group tiene consumidores activos");
            }
            if (causa instanceof InvalidPartitionsException || causa instanceof InvalidReplicationFactorException
                    || causa instanceof InvalidConfigurationException || causa instanceof PolicyViolationException
                    || causa instanceof InvalidTopicException || causa instanceof InvalidRequestException) {
                throw new SolicitudInvalidaException(causa.getMessage());
            }
            throw new IllegalStateException("Error de Kafka: " + (causa != null ? causa.getMessage() : e.getMessage()), e);
        }
    }
}
