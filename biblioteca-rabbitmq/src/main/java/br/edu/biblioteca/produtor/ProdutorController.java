package br.edu.biblioteca.produtor;

import br.edu.biblioteca.config.Topologia;
import br.edu.biblioteca.modelo.EventoBiblioteca;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** API para disparar cada caso separadamente, com dados fictícios. */
@RestController
@Profile("produtor")
@RequestMapping("/eventos")
public class ProdutorController {

    private static final List<String> CATEGORIAS =
            List.of("Computacao", "Direito", "Medicina", "Literatura", "Engenharia");
    private static final List<String> TITULOS =
            List.of("Sistemas Distribuidos", "Codigo Civil Comentado", "Anatomia Humana", "Dom Casmurro", "Resistencia dos Materiais");
    private static final List<String> LEITORES =
            List.of("Ana Souza", "Bruno Lima", "Carla Dias", "Diego Rocha", "Eva Martins");

    private final PublicadorEventos publicador;

    public ProdutorController(PublicadorEventos publicador) {
        this.publicador = publicador;
    }

    private static <T> T sorteia(List<T> lista) {
        return lista.get(ThreadLocalRandom.current().nextInt(lista.size()));
    }

    private static int n(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max);
    }

    @PostMapping("/emprestimo-registrado")
    public EventoBiblioteca emprestimo(@RequestParam(required = false) String categoria) {
        int i = n(0, TITULOS.size());
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("emprestimoId", "EMP-" + n(100, 999));
        d.put("exemplarId", "EX-" + n(1000, 9999));
        d.put("titulo", TITULOS.get(i));
        d.put("categoria", categoria != null ? categoria : CATEGORIAS.get(i));
        d.put("leitorId", "L-" + n(100, 999));
        d.put("leitorNome", sorteia(LEITORES));
        d.put("dataPrevistaDevolucao", LocalDate.now().plusDays(14).toString());
        return publicador.publicar(Topologia.RK_EMPRESTIMO, EventoBiblioteca.EMPRESTIMO_REGISTRADO, d);
    }

    @PostMapping("/exemplar-devolvido")
    public EventoBiblioteca devolucao() {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("emprestimoId", "EMP-" + n(100, 999));
        d.put("exemplarId", "EX-" + n(1000, 9999));
        d.put("leitorId", "L-" + n(100, 999));
        d.put("leitorNome", sorteia(LEITORES));
        d.put("devolvidoEm", LocalDate.now().toString());
        return publicador.publicar(Topologia.RK_DEVOLUCAO, EventoBiblioteca.EXEMPLAR_DEVOLVIDO, d);
    }

    @PostMapping("/multa-aplicada")
    public EventoBiblioteca multaAplicada() {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("multaId", "MUL-" + n(100, 999));
        d.put("emprestimoId", "EMP-" + n(100, 999));
        d.put("leitorId", "L-" + n(100, 999));
        d.put("valor", n(5, 60) + 0.50);
        d.put("motivo", "Atraso na devolucao");
        return publicador.publicar(Topologia.RK_MULTA_APLICADA, EventoBiblioteca.MULTA_APLICADA, d);
    }

    @PostMapping("/multa-cancelada")
    public EventoBiblioteca multaCancelada() {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("multaId", "MUL-" + n(100, 999));
        d.put("leitorId", "L-" + n(100, 999));
        d.put("leitorNome", sorteia(LEITORES));
        d.put("valor", n(5, 60) + 0.50);
        d.put("motivo", "Cancelamento administrativo");
        return publicador.publicar(Topologia.RK_MULTA_CANCELADA, EventoBiblioteca.MULTA_CANCELADA, d);
    }

    /** Extra de demonstração: novo fato da família "multa" (não exigido pelo enunciado). */
    @PostMapping("/multa-paga")
    public EventoBiblioteca multaPaga() {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("multaId", "MUL-" + n(100, 999));
        d.put("leitorId", "L-" + n(100, 999));
        d.put("valor", n(5, 60) + 0.50);
        return publicador.publicar(Topologia.RK_MULTA_PAGA, EventoBiblioteca.MULTA_PAGA, d);
    }

    /** Republica um evento já enviado, com o mesmo identificador. */
    @PostMapping("/republicar/{eventId}")
    public ResponseEntity<EventoBiblioteca> republicar(@PathVariable UUID eventId) {
        return publicador.republicar(eventId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
