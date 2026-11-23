package br.edu.biblioteca.consumidor;

import br.edu.biblioteca.config.Topologia;
import br.edu.biblioteca.modelo.EventoBiblioteca;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** 1. Circulação: registra a saída do exemplar (leitor + data prevista de devolução). */
@RestController
@Profile("circulacao")
public class CirculacaoConsumer {

    private static final Logger log = LoggerFactory.getLogger(CirculacaoConsumer.class);
    private final JdbcTemplate jdbc;

    public CirculacaoConsumer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @RabbitListener(queues = Topologia.Q_CIRCULACAO)
    public void receber(EventoBiblioteca e, Channel canal,
                        @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                        @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered) throws IOException {
        try {
            int novo = jdbc.update("""
                    INSERT INTO circulacao_saidas (event_id, emprestimo_id, exemplar_id, leitor_id, data_prevista_devolucao)
                    VALUES (?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING""",
                    e.eventId(), e.texto("emprestimoId"), e.texto("exemplarId"), e.texto("leitorId"),
                    Date.valueOf(LocalDate.parse(e.texto("dataPrevistaDevolucao"))));
            log.info("[circulacao] {} id={} {}", e.tipo(), e.eventId(), novo == 1 ? "saida registrada" : "duplicado ignorado");
            canal.basicAck(tag, false); // confirma SOMENTE após gravar
        } catch (Exception ex) {
            log.error("[circulacao] falha id={}: {}", e.eventId(), ex.toString());
            Ack.falha(canal, tag, redelivered);
        }
    }

    @GetMapping("/circulacao/saidas")
    public List<Map<String, Object>> saidas() {
        return jdbc.queryForList("SELECT * FROM circulacao_saidas ORDER BY registrado_em DESC LIMIT 200");
    }
}
