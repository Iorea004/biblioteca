package br.edu.biblioteca.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topologia completa do broker, declarada de forma idempotente por TODOS os processos.
 * Assim as filas (duráveis) existem desde a primeira conexão de qualquer aplicação,
 * mesmo que um consumidor ainda não tenha sido iniciado: nenhuma mensagem se perde.
 */
@Configuration
public class Topologia {

    public static final String EXCHANGE = "biblioteca.eventos";
    public static final String DLX = "biblioteca.dlx";
    public static final String DLQ = "biblioteca.dlq";

    public static final String Q_CIRCULACAO = "biblioteca.circulacao";
    public static final String Q_COMUNICACAO = "biblioteca.comunicacao";
    public static final String Q_MULTAS = "biblioteca.multas";
    public static final String Q_AUDITORIA = "biblioteca.auditoria";
    public static final String Q_ESTATISTICAS = "biblioteca.estatisticas";

    // routing keys dos eventos
    public static final String RK_EMPRESTIMO = "circulacao.emprestimo.registrado";
    public static final String RK_DEVOLUCAO = "circulacao.exemplar.devolvido";
    public static final String RK_MULTA_APLICADA = "multa.aplicada";
    public static final String RK_MULTA_CANCELADA = "multa.cancelada";
    public static final String RK_MULTA_PAGA = "multa.paga";

    private static Queue fila(String nome) {
        // durável + dead-letter: falha definitiva vai para biblioteca.dlq
        return QueueBuilder.durable(nome).deadLetterExchange(DLX).build();
    }

    @Bean
    public Declarables topologiaBiblioteca() {
        TopicExchange eventos = new TopicExchange(EXCHANGE, true, false);
        FanoutExchange dlx = new FanoutExchange(DLX, true, false);
        Queue dlq = QueueBuilder.durable(DLQ).build();

        Queue circulacao = fila(Q_CIRCULACAO);
        Queue comunicacao = fila(Q_COMUNICACAO);
        Queue multas = fila(Q_MULTAS);
        Queue auditoria = fila(Q_AUDITORIA);
        Queue estatisticas = fila(Q_ESTATISTICAS);

        Binding[] vinculos = {
            BindingBuilder.bind(dlq).to(dlx),

            // 1. Circulação: somente empréstimo registrado
            BindingBuilder.bind(circulacao).to(eventos).with(RK_EMPRESTIMO),

            // 2. Comunicação: empréstimo, devolução e multa cancelada
            BindingBuilder.bind(comunicacao).to(eventos).with(RK_EMPRESTIMO),
            BindingBuilder.bind(comunicacao).to(eventos).with(RK_DEVOLUCAO),
            BindingBuilder.bind(comunicacao).to(eventos).with(RK_MULTA_CANCELADA),

            // 3. Controle de multas: família inteira multa.* (inclui fatos futuros)
            BindingBuilder.bind(multas).to(eventos).with("multa.#"),

            // 4. Auditoria: tudo
            BindingBuilder.bind(auditoria).to(eventos).with("#"),

            // 5. Estatísticas: somente empréstimo; UMA fila compartilhada por 2 instâncias
            BindingBuilder.bind(estatisticas).to(eventos).with(RK_EMPRESTIMO)
        };

        return new Declarables(eventos, dlx, dlq, circulacao, comunicacao, multas, auditoria, estatisticas,
                vinculos[0], vinculos[1], vinculos[2], vinculos[3], vinculos[4], vinculos[5], vinculos[6], vinculos[7]);
    }
}
