package br.edu.biblioteca.consumidor;

import br.edu.biblioteca.config.Topologia;
import br.edu.biblioteca.modelo.EventoBiblioteca;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 4. Auditoria: histórico de TODOS os eventos, consultável por id, tipo e horário. */
@RestController
@Profile("auditoria")
public class AuditoriaConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditoriaConsumer.class);
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public AuditoriaConsumer(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @RabbitListener(queues = Topologia.Q_AUDITORIA)
    public void receber(EventoBiblioteca e, Channel canal,
                        @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                        @Header(AmqpHeaders.RECEIVED_ROUTING_KEY) String routingKey,
                        @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered) throws IOException {
        try {
            int novo = jdbc.update("""
                    INSERT INTO auditoria_eventos (event_id, tipo, ocorrido_em, routing_key, payload)
                    VALUES (?, ?, ?, ?, CAST(? AS jsonb)) ON CONFLICT (event_id) DO NOTHING""",
                    e.eventId(), e.tipo(), Timestamp.from(e.ocorridoEm()), routingKey, mapper.writeValueAsString(e));
            log.info("[auditoria] {} id={} rk={} {}", e.tipo(), e.eventId(), routingKey,
                    novo == 1 ? "historico gravado" : "duplicado ignorado");
            canal.basicAck(tag, false);
        } catch (Exception ex) {
            log.error("[auditoria] falha id={}: {}", e.eventId(), ex.toString());
            Ack.falha(canal, tag, redelivered);
        }
    }

    /** Ex.: /auditoria/eventos?tipo=MULTA_APLICADA&de=2026-11-18T00:00:00Z */
    @GetMapping("/auditoria/eventos")
    public List<Map<String, Object>> consultar(@RequestParam(required = false) UUID id,
                                               @RequestParam(required = false) String tipo,
                                               @RequestParam(required = false) String de,
                                               @RequestParam(required = false) String ate) {
        StringBuilder sql = new StringBuilder(
                "SELECT event_id, tipo, ocorrido_em, routing_key, payload::text AS payload FROM auditoria_eventos WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (id != null) { sql.append(" AND event_id = ?"); args.add(id); }
        if (tipo != null) { sql.append(" AND tipo = ?"); args.add(tipo); }
        if (de != null) { sql.append(" AND ocorrido_em >= ?"); args.add(Timestamp.from(Instant.parse(de))); }
        if (ate != null) { sql.append(" AND ocorrido_em <= ?"); args.add(Timestamp.from(Instant.parse(ate))); }
        sql.append(" ORDER BY ocorrido_em DESC LIMIT 200");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }
}
