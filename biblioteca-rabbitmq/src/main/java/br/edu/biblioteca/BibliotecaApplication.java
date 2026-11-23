package br.edu.biblioteca;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Uma única base de código, seis papéis independentes escolhidos por perfil Spring:
 * produtor, circulacao, comunicacao, multas, auditoria, estatisticas.
 * Cada perfil é um processo separado (java -jar ... --spring.profiles.active=X).
 */
@SpringBootApplication
public class BibliotecaApplication {
    public static void main(String[] args) {
        SpringApplication.run(BibliotecaApplication.class, args);
    }
}
