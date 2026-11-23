package br.edu.biblioteca.produtor;

import br.edu.biblioteca.config.Topologia;
import br.edu.biblioteca.modelo.EventoBiblioteca;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("produtor")
public class PublicadorEventos implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(PublicadorEventos.class);

    private record Publicado(String routingKey, EventoBiblioteca evento) {}

    private final RabbitTemplate rabbit;
    private final Map<UUID, Publicado> historico = new ConcurrentHashMap<>();

    public PublicadorEventos(RabbitTemplate rabbit) {
        this.rabbit = rabbit;
    }

    /** Abre a conexão na partida para que a topologia (filas e vínculos) seja declarada antes do 1º evento. */
    @Override
    public void run(ApplicationArguments args) {
        rabbit.execute(channel -> null);
        log.info("Topologia declarada. Produtor pronto.");
    }

    public EventoBiblioteca publicar(String routingKey, String tipo, Map<String, Object> dados) {
        EventoBiblioteca evento = new EventoBiblioteca(UUID.randomUUID(), tipo, Instant.now(), dados);
        enviar(routingKey, evento);
        historico.put(evento.eventId(), new Publicado(routingKey, evento));
        return evento;
    }

    /** Reenvia o MESMO evento (mesmo eventId e mesmo conteúdo): teste de duplicidade. */
    public Optional<EventoBiblioteca> republicar(UUID eventId) {
        Publicado p = historico.get(eventId);
        if (p == null) {
            return Optional.empty();
        }
        enviar(p.routingKey(), p.evento());
        return Optional.of(p.evento());
    }

    private void enviar(String routingKey, EventoBiblioteca evento) {
        rabbit.convertAndSend(Topologia.EXCHANGE, routingKey, evento);
        log.info("Publicado {} id={} rk={}", evento.tipo(), evento.eventId(), routingKey);
    }
}
