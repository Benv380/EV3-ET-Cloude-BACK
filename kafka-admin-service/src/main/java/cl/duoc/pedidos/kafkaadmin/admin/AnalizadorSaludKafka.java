package cl.duoc.pedidos.kafkaadmin.admin;

import cl.duoc.pedidos.kafkaadmin.admin.dto.Alerta;
import cl.duoc.pedidos.kafkaadmin.admin.dto.GrupoResumen;
import cl.duoc.pedidos.kafkaadmin.admin.dto.SaludKafka;
import cl.duoc.pedidos.kafkaadmin.admin.dto.TopicoResumen;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Convierte el estado del clúster en alertas explicadas (qué pasa y qué impacto tiene).
 * Lógica pura, sin AdminClient: se prueba con datos de ejemplo.
 */
public class AnalizadorSaludKafka {

    private final int brokersEsperados;
    private final long umbralLag;
    private final int minInsync;

    public AnalizadorSaludKafka(int brokersEsperados, long umbralLag, int minInsync) {
        this.brokersEsperados = brokersEsperados;
        this.umbralLag = umbralLag;
        this.minInsync = minInsync;
    }

    public SaludKafka analizar(int brokersActivos, List<TopicoResumen> topicos, List<GrupoResumen> grupos,
                               int particionesSinLider, String topicoDlt) {
        List<Alerta> alertas = new ArrayList<>();

        if (brokersActivos < minInsync) {
            alertas.add(new Alerta(Alerta.Nivel.CRITICO, "cluster",
                    brokersActivos + " de " + brokersEsperados + " brokers activos",
                    "Con menos de " + minInsync + " brokers no se cumple min.insync.replicas: los productores con"
                            + " acks=all reciben error y NO se pueden publicar eventos."));
        } else if (brokersActivos < brokersEsperados) {
            alertas.add(new Alerta(Alerta.Nivel.ADVERTENCIA, "cluster",
                    brokersActivos + " de " + brokersEsperados + " brokers activos",
                    "El sistema sigue funcionando (las réplicas eligieron nuevos líderes), pero ya no tolera"
                            + " otra caída. Levantar el broker caído."));
        }

        if (particionesSinLider > 0) {
            alertas.add(new Alerta(Alerta.Nivel.CRITICO, "particiones",
                    particionesSinLider + " particiones sin líder",
                    "Esas particiones no aceptan lecturas ni escrituras hasta que vuelva un broker con su réplica."));
        }

        int subReplicadas = 0;
        long mensajesDlt = 0;
        for (TopicoResumen t : topicos) {
            subReplicadas += t.particionesSubReplicadas();
            if (t.particionesSubReplicadas() > 0) {
                alertas.add(new Alerta(Alerta.Nivel.ADVERTENCIA, t.nombre(),
                        t.particionesSubReplicadas() + " particiones sub-replicadas (ISR < réplicas)",
                        "Hay copias atrasadas o en un broker caído: menos tolerancia a fallos hasta que se sincronicen."));
            }
            if (t.nombre().equals(topicoDlt)) {
                mensajesDlt = t.mensajes();
            }
        }

        Map<String, Long> lagPorGrupo = new LinkedHashMap<>();
        for (GrupoResumen g : grupos) {
            lagPorGrupo.put(g.grupo(), g.lagTotal());
            boolean sinConsumidores = g.miembros() == 0;
            if (sinConsumidores && g.lagTotal() > 0) {
                alertas.add(new Alerta(Alerta.Nivel.ADVERTENCIA, g.grupo(),
                        "Grupo sin consumidores activos con " + g.lagTotal() + " mensajes pendientes",
                        "Los eventos se acumulan (no se pierden mientras estén dentro de la retención). Si la retención"
                                + " vence antes de que vuelva el consumidor, esos eventos se borran sin procesar."));
            } else if (g.lagTotal() > umbralLag) {
                alertas.add(new Alerta(Alerta.Nivel.ADVERTENCIA, g.grupo(),
                        "Lag alto: " + g.lagTotal() + " mensajes pendientes",
                        "El grupo consume más lento de lo que se produce: aumenta la latencia del tiempo real."
                                + " Agregar consumidores (hasta 1 por partición) o aumentar particiones."));
            }
        }

        if (mensajesDlt > 0) {
            alertas.add(new Alerta(Alerta.Nivel.INFO, topicoDlt, mensajesDlt + " mensajes en el DLT",
                    "No frenan el flujo, pero son eventos con error que se deben revisar."));
        }

        String estado = alertas.stream().anyMatch(a -> a.nivel() == Alerta.Nivel.CRITICO) ? "CRITICO"
                : alertas.stream().anyMatch(a -> a.nivel() == Alerta.Nivel.ADVERTENCIA) ? "ADVERTENCIA" : "OK";
        return new SaludKafka(estado, brokersActivos, brokersEsperados, subReplicadas, particionesSinLider,
                lagPorGrupo, mensajesDlt, alertas);
    }
}
