package cl.duoc.pedidos.streaming;

import cl.duoc.pedidos.streaming.error.ErrorTransitorioException;
import cl.duoc.pedidos.streaming.evento.EventoPedidoStream;
import cl.duoc.pedidos.streaming.evento.TipoEventoPedido;
import cl.duoc.pedidos.streaming.produccion.ProductorEventos;
import cl.duoc.pedidos.streaming.validacion.ValidadorEventos;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Prueba de punta a punta con 3 brokers Kafka embebidos:
 * topics creados con 3 particiones x 3 réplicas, productor con DTO + headers, consumidor @KafkaListener,
 * reintentos de errores recuperables y envío al DLT de payloads inválidos y JSON corrupto.
 */
@SpringBootTest(classes = {FlujoKafkaIntegracionTest.App.class, FlujoKafkaIntegracionTest.Listeners.class}, properties = {
        "spring.config.import=classpath:streaming.yml",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "streaming.reintentos.maximo=2",
        "streaming.reintentos.espera-inicial-ms=100",
        "streaming.reintentos.espera-maxima-ms=200"})
@EmbeddedKafka(count = 3)
class FlujoKafkaIntegracionTest {

    @SpringBootApplication
    static class App {
    }

    @TestConfiguration
    static class Listeners {
        static final BlockingQueue<ConsumerRecord<String, EventoPedidoStream>> recibidos = new LinkedBlockingQueue<>();
        static final BlockingQueue<ConsumerRecord<String, String>> dlt = new LinkedBlockingQueue<>();
        static final AtomicInteger intentosFallidos = new AtomicInteger();

        @Bean
        Object listenerPrueba(ValidadorEventos validador) {
            return new Object() {
                @KafkaListener(id = "prueba", topics = "pedidos.eventos", groupId = "prueba",
                        properties = "spring.json.value.default.type=cl.duoc.pedidos.streaming.evento.EventoPedidoStream")
                void recibir(ConsumerRecord<String, EventoPedidoStream> r) {
                    validador.validar(r.value());
                    if (r.value().simula("ANALITICA")) {
                        intentosFallidos.incrementAndGet();
                        throw new ErrorTransitorioException("simulado");
                    }
                    recibidos.add(r);
                }

                @KafkaListener(id = "prueba-dlt", topics = "pedidos.dlt", groupId = "prueba-dlt",
                        properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
                void dlt(ConsumerRecord<String, String> r) {
                    dlt.add(r);
                }
            };
        }
    }

    @Autowired
    ProductorEventos productor;
    @Autowired
    KafkaAdmin kafkaAdmin;
    @Value("${spring.embedded.kafka.brokers}")
    String brokers;

    private static EventoPedidoStream evento(long pedidoId, String fallo) {
        return new EventoPedidoStream(UUID.randomUUID(), 1, TipoEventoPedido.PEDIDO_CREADO, pedidoId, "Ana",
                new BigDecimal("1500"), "CREADO", null, "ana", "test", fallo, Instant.now());
    }

    @Test
    void flujoCompleto() throws Exception {
        // 1) Topics con 3 particiones, 3 réplicas y su política de limpieza
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Map<String, TopicDescription> d = admin.describeTopics(List.of("pedidos.eventos", "pedidos.estados"))
                    .allTopicNames().get(10, TimeUnit.SECONDS);
            assertThat(d.get("pedidos.eventos").partitions()).hasSize(3)
                    .allSatisfy(p -> assertThat(p.replicas()).hasSize(3));
            var conf = admin.describeConfigs(List.of(new ConfigResource(ConfigResource.Type.TOPIC, "pedidos.estados")))
                    .all().get(10, TimeUnit.SECONDS).values().iterator().next();
            assertThat(conf.get("cleanup.policy").value()).isEqualTo("compact");
        }

        // 2) Evento válido: llega al consumidor con su clave y headers de negocio
        EventoPedidoStream ok = evento(7, "NINGUNA");
        var meta = productor.publicar("pedidos.eventos", ok.clave(), ok, Map.of("tipo-evento", "PEDIDO_CREADO"))
                .get(10, TimeUnit.SECONDS).getRecordMetadata();
        ConsumerRecord<String, EventoPedidoStream> r = Listeners.recibidos.poll(30, TimeUnit.SECONDS);
        assertThat(r).isNotNull();
        assertThat(r.key()).isEqualTo("7");
        assertThat(r.partition()).isEqualTo(meta.partition());
        assertThat(r.value().eventoId()).isEqualTo(ok.eventoId());
        assertThat(new String(r.headers().lastHeader("tipo-evento").value(), StandardCharsets.UTF_8))
                .isEqualTo("PEDIDO_CREADO");

        // 3) Error recuperable: 1 intento + 2 reintentos y luego al DLT
        productor.publicar("pedidos.eventos", "8", evento(8, "ANALITICA")).get(10, TimeUnit.SECONDS);
        ConsumerRecord<String, String> enDlt = Listeners.dlt.poll(30, TimeUnit.SECONDS);
        assertThat(enDlt).isNotNull();
        assertThat(enDlt.key()).isEqualTo("8");
        assertThat(Listeners.intentosFallidos.get()).isEqualTo(3);
        assertThat(texto(enDlt, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("simulado");

        // 4) JSON corrupto: no bota al consumidor, va directo al DLT
        KafkaTemplate<String, String> texto = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));
        texto.send("pedidos.eventos", "9", "{ esto no es json").get(10, TimeUnit.SECONDS);
        ConsumerRecord<String, String> corrupto = Listeners.dlt.poll(30, TimeUnit.SECONDS);
        assertThat(corrupto).isNotNull();
        assertThat(corrupto.value()).isEqualTo("{ esto no es json");
        assertThat(texto(corrupto, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo("pedidos.eventos");

        // 5) El consumidor sigue vivo después de los errores
        EventoPedidoStream despues = evento(10, "NINGUNA");
        productor.publicar("pedidos.eventos", despues.clave(), despues).get(10, TimeUnit.SECONDS);
        assertThat(Listeners.recibidos.poll(30, TimeUnit.SECONDS)).isNotNull();
    }

    private static String texto(ConsumerRecord<?, ?> r, String header) {
        var h = r.headers().lastHeader(header);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
