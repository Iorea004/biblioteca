package br.edu.biblioteca.consumidor;

import com.rabbitmq.client.Channel;
import java.io.IOException;

final class Ack {
    private Ack() {}

    /**
     * Falha no processamento: 1ª vez devolve à fila (nova tentativa);
     * se já era reentrega, descarta para a DLQ (evita loop infinito).
     */
    static void falha(Channel canal, long tag, Boolean redelivered) throws IOException {
        boolean reenfileirar = !Boolean.TRUE.equals(redelivered);
        canal.basicNack(tag, false, reenfileirar);
    }
}
