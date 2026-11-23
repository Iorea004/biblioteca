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
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** 2. Comunicação com leitores: registra avisos (simula o envio de e-mail/SMS). */
@RestController
@Profile("comunicacao")
public class ComunicacaoConsumer {

    private static final Logger log = LoggerFactory.getLogger(ComunicacaoConsumer.class);
    private final JdbcTemplate jdbc;

    public ComunicacaoConsumer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @RabbitListener(queues = Topologia.Q_COMUNICACAO)
    public void receber(EventoBiblioteca e, Channel canal,
                        @Header(AmqpHeaders.DELIVERY_TAG) long tag,
                        @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered) throws IOException {
        try {
            String nome = e.texto("leitorNome");
            String tipoAviso;
            String mensagem;
            switch (e.tipo()) {
                case EventoBiblioteca.EMPRESTIMO_REGISTRADO -> {
                    tipoAviso = "EMPRESTIMO";
                    mensagem = "Ola " + nome + ", emprestimo do exemplar " + e.texto("exemplarId")
                            + " registrado. Devolucao prevista ate " + e.texto("dataPrevistaDevolucao") + ".";
                }
                case EventoBiblioteca.EXEMPLAR_DEVOLVIDO -> {
                    tipoAviso = "DEVOLUCAO";
                    mensagem = "Ola " + nome + ", recebemos a devolucao do exemplar " + e.texto("exemplarId") + ".";
                }
                case EventoBiblioteca.MULTA_CANCELADA -> {
                    tipoAviso = "MULTA_CANCELADA";
                    mensagem = "Ola " + nome + ", a multa " + e.texto("multaId") + " de R$ " + e.texto("valor")
                            + " foi cancelada.";
                }
                default -> {
                    tipoAviso = "OUTRO";
                    mensagem = "Evento " + e.tipo() + " sem aviso especifico.";
                }
            }
            int novo = jdbc.update("""
                    INSERT INTO comunicacao_avisos (event_id, tipo_aviso, leitor_id, leitor_nome, mensagem)
                    VALUES (?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING""",
                    e.eventId(), tipoAviso, e.texto("leitorId"), nome, mensagem);
            log.info("[comunicacao] {} id={} {}", e.tipo(), e.eventId(), novo == 1 ? "aviso registrado" : "duplicado ignorado");
            canal.basicAck(tag, false);
        } catch (Exception ex) {
            log.error("[comunicacao] falha id={}: {}", e.eventId(), ex.toString());
            Ack.falha(canal, tag, redelivered);
        }
    }

    @GetMapping("/comunicacao/avisos")
    public List<Map<String, Object>> avisos() {
        return jdbc.queryForList("SELECT * FROM comunicacao_avisos ORDER BY registrado_em DESC LIMIT 200");
    }
}
