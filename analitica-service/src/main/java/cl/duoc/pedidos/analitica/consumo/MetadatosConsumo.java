package cl.duoc.pedidos.analitica.consumo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Datos de Kafka de un registro consumido (topic, partición, offset y qué consumidor del grupo lo procesó).
 * Permite que la lógica de negocio registre la distribución de carga sin depender de clases de Kafka.
 */
public record MetadatosConsumo(String topico, int particion, long offset, String grupo, String consumidor) {

    public static MetadatosConsumo de(ConsumerRecord<?, ?> registro, String grupo) {
        return new MetadatosConsumo(registro.topic(), registro.partition(), registro.offset(), grupo,
                nombreConsumidor(Thread.currentThread().getName()));
    }

    /**
     * Spring nombra cada hilo consumidor como "&lt;id-listener&gt;-&lt;n&gt;-C-1"; "n" identifica al consumidor
     * dentro del grupo (0, 1, 2 con concurrencia 3).
     */
    static String nombreConsumidor(String hilo) {
        int fin = hilo.lastIndexOf("-C-");
        if (fin <= 0) {
            return hilo;
        }
        int inicio = hilo.lastIndexOf('-', fin - 1);
        return inicio < 0 ? hilo : "consumidor-" + hilo.substring(inicio + 1, fin);
    }
}
