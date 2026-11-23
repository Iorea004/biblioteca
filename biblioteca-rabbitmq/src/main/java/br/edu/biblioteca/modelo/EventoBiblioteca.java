package br.edu.biblioteca.modelo;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Contrato JSON dos eventos (um fato ocorrido, não uma ordem).
 * "dados" é um mapa aberto: novos fatos da mesma família não quebram consumidores existentes.
 */
public record EventoBiblioteca(UUID eventId, String tipo, Instant ocorridoEm, Map<String, Object> dados) {

    public static final String EMPRESTIMO_REGISTRADO = "EMPRESTIMO_REGISTRADO";
    public static final String EXEMPLAR_DEVOLVIDO = "EXEMPLAR_DEVOLVIDO";
    public static final String MULTA_APLICADA = "MULTA_APLICADA";
    public static final String MULTA_CANCELADA = "MULTA_CANCELADA";
    public static final String MULTA_PAGA = "MULTA_PAGA"; // extra: demonstra "novo fato da família"

    public String texto(String chave) {
        Object v = dados == null ? null : dados.get(chave);
        return v == null ? null : String.valueOf(v);
    }

    public BigDecimal decimal(String chave) {
        String t = texto(chave);
        return t == null ? null : new BigDecimal(t);
    }
}
