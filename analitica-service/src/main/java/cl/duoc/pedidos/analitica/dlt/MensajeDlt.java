package cl.duoc.pedidos.analitica.dlt;

/** Un mensaje que terminó en pedidos.dlt, con el motivo y su ubicación original. */
public record MensajeDlt(
        int particionDlt,
        long offsetDlt,
        String clave,
        String topicoOriginal,
        Integer particionOriginal,
        Long offsetOriginal,
        String grupoOriginal,
        String excepcion,
        String motivo,
        String payload,
        String fecha) {
}
