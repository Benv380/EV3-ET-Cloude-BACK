package cl.duoc.pedidos.streaming.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Map;

/**
 * Propiedades de streaming leídas desde streaming.yml (prefijo "streaming").
 * Ninguna clase de negocio tiene nombres de topics o grupos escritos a mano.
 */
@Validated
@ConfigurationProperties(prefix = "streaming")
public record StreamingProperties(
        boolean crearTopicos,
        @NotNull @Valid Topicos topicos,
        @NotNull @Valid Grupos grupos,
        @NotNull @Valid Reintentos reintentos) {

    public record Topicos(@NotNull @Valid Topico eventos, @NotNull @Valid Topico estados, @NotNull @Valid Topico dlt) {

        public List<Topico> todos() {
            return List.of(eventos, estados, dlt);
        }

        /** Topics del sistema: el microservicio de administración no permite eliminarlos. */
        public boolean esProtegido(String nombre) {
            return todos().stream().anyMatch(t -> t.nombre().equals(nombre));
        }
    }

    public record Topico(
            @NotBlank String nombre,
            @Min(1) @Max(50) int particiones,
            @Min(1) @Max(5) short replicas,
            Map<String, String> configs) {

        public Topico {
            configs = configs == null ? Map.of() : Map.copyOf(configs);
        }

        public NewTopic nuevo() {
            return TopicBuilder.name(nombre).partitions(particiones).replicas(replicas).configs(configs).build();
        }
    }

    public record Grupos(@NotBlank String analitica, @NotBlank String alertas, @NotBlank String estados,
                         @NotBlank String dlt) {

        public List<String> todos() {
            return List.of(analitica, alertas, estados, dlt);
        }
    }

    public record Reintentos(@Min(0) @Max(10) int maximo, @Min(100) long esperaInicialMs,
                             @DecimalMin("1.0") double multiplicador, @Min(100) long esperaMaximaMs) {
    }
}
