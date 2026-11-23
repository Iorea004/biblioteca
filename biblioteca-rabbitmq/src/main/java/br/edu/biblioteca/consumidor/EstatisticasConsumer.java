package br.edu.biblioteca.consumidor;

import br.edu.biblioteca.config.Topologia;
import br.edu.biblioteca.modelo.EventoBiblioteca;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 5. Estatísticas de acervo: contador de empréstimos por categoria.
 * Duas instâncias consomem a MESMA fila (competing consumers): cada mensagem vai para uma só.
 * Idempotência: marca do eventId + incremento na mesma transação.
 */
@RestController
@Profile("estatisticas")
public class EstatisticasConsumer {

    private static final Logger log = LoggerFactory.getLogger(EstatisticasConsumer.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final String instancia;
    private final long atrasoMs;

    public EstatisticasConsumer(JdbcTemplate jdbc, TransactionTemplate tx,
                                @Value("${app.instancia}") String instancia,
                                @Value("${app.processamento-ms:0}") long atrasoMs) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.instancia = instancia;
        this.atrasoMs = atrasoMs;
    }

    @RabbitListener(queues = Topologia.Q_ESTATISTICAS)
    public void receber(EventoBiblioteca e, Channel canal,
                        @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                        @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered) throws IOException {
        try {
            if (atrasoMs > 0) {
                Thread.sleep(atrasoMs); // simula trabalho para tornar a divisão visível
            }
            String categoria = e.texto("categoria");
            Boolean contou = tx.execute(status -> {
                int novo = jdbc.update("""
                        INSERT INTO estatisticas_processados (event_id, instancia) VALUES (?, ?)
                        ON CONFLICT (event_id) DO NOTHING""", e.eventId(), instancia);
                if (novo == 0) {
                    return false; // já processado: não incrementa
                }
                jdbc.update("""
                        INSERT INTO estatisticas_categoria (categoria, total) VALUES (?, 1)
                        ON CONFLICT (categoria) DO UPDATE SET total = estatisticas_categoria.total + 1""", categoria);
                return true;
            });
            log.info("[estatisticas:{}] id={} categoria={} {}", instancia, e.eventId(), categoria,
                    Boolean.TRUE.equals(contou) ? "contabilizado" : "DUPLICADO ignorado (sem novo incremento)");
            canal.basicAck(tag, false); // após commit
        } catch (Exception ex) {
            log.error("[estatisticas:{}] falha id={}: {}", instancia, e.eventId(), ex.toString());
            Ack.falha(canal, tag, redelivered);
        }
    }

    @GetMapping("/estatisticas/categorias")
    public List<Map<String, Object>> categorias() {
        return jdbc.queryForList("SELECT categoria, total FROM estatisticas_categoria ORDER BY categoria");
    }

    /** Mostra qual instância processou cada evento. */
    @GetMapping("/estatisticas/processamento")
    public List<Map<String, Object>> processamento() {
        return jdbc.queryForList("SELECT event_id, instancia, processado_em FROM estatisticas_processados ORDER BY processado_em");
    }
}
