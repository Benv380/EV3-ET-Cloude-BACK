package cl.duoc.pedidos.kafkaadmin.admin;

import cl.duoc.pedidos.common.web.SolicitudInvalidaException;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Reglas para crear y modificar topics. Evita configuraciones peligrosas antes de llegar al broker:
 * nombres inválidos, más réplicas que brokers, min.insync.replicas imposible de cumplir, retenciones absurdas.
 */
public class ValidadorTopicos {

    private static final Pattern NOMBRE = Pattern.compile("[a-zA-Z0-9._-]{1,200}");

    /** Configuraciones que se permite cambiar desde la API (las demás quedan con el valor del broker). */
    public static final Set<String> CONFIGS_PERMITIDAS = Set.of(
            "cleanup.policy", "retention.ms", "retention.bytes", "min.insync.replicas", "segment.ms",
            "segment.bytes", "max.message.bytes", "compression.type", "min.cleanable.dirty.ratio",
            "delete.retention.ms", "min.compaction.lag.ms");

    private static final Set<String> POLITICAS = Set.of("delete", "compact", "compact,delete", "delete,compact");
    private static final Set<String> COMPRESIONES = Set.of("producer", "uncompressed", "gzip", "snappy", "lz4", "zstd");
    private static final long UN_MINUTO = 60_000L;

    private final int maxParticiones;

    public ValidadorTopicos(int maxParticiones) {
        this.maxParticiones = maxParticiones;
    }

    public void validarCreacion(String nombre, int particiones, int replicas, int brokersActivos,
                                Map<String, String> configs) {
        validarNombre(nombre);
        if (particiones < 1 || particiones > maxParticiones) {
            throw new SolicitudInvalidaException("Las particiones deben estar entre 1 y " + maxParticiones);
        }
        if (replicas < 1 || replicas > brokersActivos) {
            throw new SolicitudInvalidaException("Las réplicas deben estar entre 1 y " + brokersActivos
                    + " (no puede haber más copias que brokers activos)");
        }
        validarConfigs(configs, replicas);
    }

    public void validarNombre(String nombre) {
        if (nombre == null || !NOMBRE.matcher(nombre).matches() || nombre.startsWith("__")
                || nombre.equals(".") || nombre.equals("..")) {
            throw new SolicitudInvalidaException("Nombre de topic inválido: use letras, números, '.', '_' o '-'"
                    + " (máx. 200) y no empiece con '__' (reservado para topics internos)");
        }
    }

    public void validarConfigs(Map<String, String> configs, int replicas) {
        configs.forEach((clave, valor) -> validarConfig(clave, valor, replicas));
    }

    public void validarConfig(String clave, String valor, int replicas) {
        if (!CONFIGS_PERMITIDAS.contains(clave)) {
            throw new SolicitudInvalidaException("Configuración no permitida: " + clave + ". Permitidas: "
                    + CONFIGS_PERMITIDAS.stream().sorted().toList());
        }
        if (valor == null || valor.isBlank()) {
            throw new SolicitudInvalidaException("Valor vacío para " + clave);
        }
        String v = valor.trim();
        switch (clave) {
            case "cleanup.policy" -> {
                if (!POLITICAS.contains(v.replace(" ", ""))) {
                    throw new SolicitudInvalidaException("cleanup.policy debe ser delete, compact o compact,delete");
                }
            }
            case "compression.type" -> {
                if (!COMPRESIONES.contains(v)) {
                    throw new SolicitudInvalidaException("compression.type inválido. Valores: " + COMPRESIONES);
                }
            }
            case "min.insync.replicas" -> {
                long n = numero(clave, v);
                if (n < 1 || n > replicas) {
                    throw new SolicitudInvalidaException("min.insync.replicas debe estar entre 1 y " + replicas
                            + " (réplicas del topic); si es mayor, ninguna escritura con acks=all sería aceptada");
                }
            }
            case "retention.ms" -> {
                long n = numero(clave, v);
                if (n != -1 && n < UN_MINUTO) {
                    throw new SolicitudInvalidaException("retention.ms debe ser -1 (sin límite) o al menos 60000 (1 min)");
                }
            }
            case "retention.bytes" -> {
                long n = numero(clave, v);
                if (n != -1 && n < 1_048_576) {
                    throw new SolicitudInvalidaException("retention.bytes debe ser -1 (sin límite) o al menos 1 MB");
                }
            }
            case "segment.ms", "delete.retention.ms", "min.compaction.lag.ms" -> {
                if (numero(clave, v) < (clave.equals("segment.ms") ? UN_MINUTO : 0)) {
                    throw new SolicitudInvalidaException(clave + " fuera de rango");
                }
            }
            case "segment.bytes" -> {
                if (numero(clave, v) < 1_048_576) {
                    throw new SolicitudInvalidaException("segment.bytes debe ser al menos 1 MB");
                }
            }
            case "max.message.bytes" -> {
                long n = numero(clave, v);
                if (n < 1024 || n > 10_485_760) {
                    throw new SolicitudInvalidaException("max.message.bytes debe estar entre 1 KB y 10 MB");
                }
            }
            case "min.cleanable.dirty.ratio" -> {
                double d;
                try {
                    d = Double.parseDouble(v);
                } catch (NumberFormatException e) {
                    throw new SolicitudInvalidaException(clave + " debe ser un número entre 0 y 1");
                }
                if (d < 0 || d > 1) {
                    throw new SolicitudInvalidaException(clave + " debe estar entre 0 y 1");
                }
            }
            default -> {
                // permitida sin regla extra
            }
        }
    }

    public void validarAumentoParticiones(int actuales, int nuevas) {
        if (nuevas <= actuales) {
            throw new SolicitudInvalidaException("Kafka solo permite AUMENTAR particiones (actuales: " + actuales
                    + "). Para reducirlas hay que crear un topic nuevo.");
        }
        if (nuevas > maxParticiones) {
            throw new SolicitudInvalidaException("Máximo " + maxParticiones + " particiones");
        }
    }

    private static long numero(String clave, String valor) {
        try {
            return Long.parseLong(valor);
        } catch (NumberFormatException e) {
            throw new SolicitudInvalidaException(clave + " debe ser un número entero");
        }
    }
}
