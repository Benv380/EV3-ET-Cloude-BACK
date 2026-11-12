package cl.duoc.pedidos.kafkaadmin.admin.dto;

import java.util.List;

/**
 * @param lider       broker líder (atiende lecturas/escrituras); null = partición sin líder (no disponible)
 * @param replicas    brokers que guardan una copia
 * @param isr         réplicas en sincronía (In-Sync Replicas)
 * @param mensajes    offsetFinal - offsetInicial = mensajes retenidos hoy en la partición
 */
public record ParticionInfo(int particion, Integer lider, List<Integer> replicas, List<Integer> isr,
                            long offsetInicial, long offsetFinal, long mensajes, boolean subReplicada) {
}
