package br.edu.biblioteca.consumidor;

import br.edu.biblioteca.config.Topologia;
import br.edu.biblioteca.modelo.EventoBiblioteca;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** 3. Controle de multas: registra multas (aplicadas, canceladas e qualquer novo fato multa.*). */
@RestController
@Profile("multas")
public class ControleMultasConsumer {

    private static final Logger log = LoggerFactory.getLogger(ControleMultasConsumer.class);
    private final JdbcTemplate jdbc;

    public ControleMultasConsumer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @RabbitListener(queues = Topologia.Q_MULTAS)
    public void receber(EventoBiblioteca e, Channel canal,
                        @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                        @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered) throws IOException {
        try {
            // MULTA_APLICADA -> APLICADA; MULTA_CANCELADA -> CANCELADA; MULTA_PAGA -> PAGA ...
            String situacao = e.tipo().startsWith("MULTA_") ? e.tipo().substring(6) : e.tipo();
            int novo = jdbc.update("""
                    INSERT INTO multas_registro (event_id, multa_id, situacao, leitor_id, valor, ocorrido_em)
                    VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING""",
                    e.eventId(), e.texto("multaId"), situacao, e.texto("leitorId"), e.decimal("valor"),
                    Timestamp.from(e.ocorridoEm()));
            log.info("[multas] {} id={} {}", e.tipo(), e.eventId(), novo == 1 ? "registrada" : "duplicado ignorado");
            canal.basicAck(tag, false);
        } catch (Exception ex) {
            log.error("[multas] falha id={}: {}", e.eventId(), ex.toString());
            Ack.falha(canal, tag, redelivered);
        }
    }

    @GetMapping("/multas")
    public List<Map<String, Object>> multas() {
        return jdbc.queryForList("SELECT * FROM multas_registro ORDER BY registrado_em DESC LIMIT 200");
    }
}
