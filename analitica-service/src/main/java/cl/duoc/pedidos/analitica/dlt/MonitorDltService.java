package cl.duoc.pedidos.analitica.dlt;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Guarda en memoria los mensajes del DLT (el consumidor relee el topic al iniciar, así que se reconstruye solo).
 * Clave interna = partición + offset del DLT: releer el mismo mensaje no lo duplica.
 */
@Service
public class MonitorDltService {

    private static final int MAXIMO = 200;

    private final Map<String, MensajeDlt> mensajes = Collections.synchronizedMap(new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, MensajeDlt> mayor) {
            return size() > MAXIMO;
        }
    });

    public void registrar(MensajeDlt mensaje) {
        mensajes.put(mensaje.particionDlt() + "@" + mensaje.offsetDlt(), mensaje);
    }

    public long total() {
        return mensajes.size();
    }

    public List<MensajeDlt> ultimos() {
        List<MensajeDlt> copia;
        synchronized (mensajes) {
            copia = new ArrayList<>(mensajes.values());
        }
        copia.sort(Comparator.comparing(MensajeDlt::fecha).reversed());
        return copia.stream().limit(50).toList();
    }
}
