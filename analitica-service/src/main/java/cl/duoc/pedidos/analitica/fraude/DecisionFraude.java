package cl.duoc.pedidos.analitica.fraude;

/** Resultado de evaluar un evento contra las reglas de fraude. */
public record DecisionFraude(String regla, String motivo, int pedidosEnVentana) {

    public static final String RAFAGA = "RAFAGA_DE_PEDIDOS";
    public static final String MONTO_EXCESIVO = "MONTO_EXCESIVO";
}
