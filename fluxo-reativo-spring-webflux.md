
# Construindo um Fluxo Reativo Completo com Spring WebFlux e WebClient

No primeiro artigo desta série, exploramos o papel do `subscribe()` no Spring WebFlux e como ele dispara operações reativas, como chamadas HTTP com WebClient. Agora, vamos dar um passo adiante e construir um fluxo reativo completo, desde o controller até o serviço, utilizando o WebClient para fazer requisições HTTP de forma não-bloqueante. Mostraremos como o Spring WebFlux gerencia a execução automaticamente, eliminando a necessidade de chamar `.subscribe()` manualmente, e como implementar logging de maneira eficiente.

## O Objetivo: Um Fluxo Reativo para Criar Recursos

Nosso cenário é simples: queremos criar uma API REST que permita criar um recurso do tipo `Beer` (cerveja) enviando uma requisição POST para um endpoint externo. A aplicação deve:

- Receber a solicitação via um controller.
- Delegar a lógica de negócio a um serviço.
- Usar o WebClient para fazer a requisição HTTP.
- Registrar o resultado no log sem bloquear threads.
- Manter o fluxo 100% reativo.

## Passo 1: Definindo o Modelo de Dados

```java
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Beer {
    private UUID id;
    private Integer version;
    private String beerName;
    private BeerStyle beerStyle;
    private String upc;
    private BigDecimal price;
    private Integer quantityOnHand;
    private LocalDateTime createdDate;
    private LocalDateTime updateDate;
}

public enum BeerStyle {
    PALE_ALE, IPA, LAGER, STOUT
}
```

## Passo 2: Criando o Serviço com WebClient

```java
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class BeerService {

    private final WebClient webClient;
    private final Logger log = LoggerFactory.getLogger(BeerService.class);

    public Mono<String> createBeer() {
        Beer beer = Beer.builder()
                .id(UUID.randomUUID())
                .version(1)
                .beerName("Create")
                .beerStyle(BeerStyle.PALE_ALE)
                .upc("12356222")
                .price(new BigDecimal("11.99"))
                .quantityOnHand(392)
                .createdDate(LocalDateTime.now())
                .updateDate(LocalDateTime.now())
                .build();

        return webClient.post()
                .uri("/beer")
                .bodyValue(beer)
                .exchangeToMono(response -> {
                    if (response.statusCode().is2xxSuccessful()) {
                        return Mono.just("tudo certo! " + response.statusCode().value());
                    } else {
                        return Mono.just("something went wrong! " + response.statusCode().value());
                    }
                })
                .doOnNext(result -> log.info("Resultado da chamada WebClient: {}", result));
    }
}
```

## Passo 3: Implementando o Controller

```java
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/beers")
@RequiredArgsConstructor
public class BeerController {

    private final BeerService beerService;

    @PostMapping("/create")
    public Mono<ResponseEntity<String>> createBeer() {
        return beerService.createBeer()
                .map(response -> ResponseEntity.ok(response));
    }
}
```

## Passo 4: Configurando o WebClient

```java
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

    @Bean
    public WebClient webClient() {
        return WebClient.create("http://localhost:8081");
    }
}
```

## Como Tudo se Encaixa

Quando um cliente faz uma requisição POST para `/api/beers/create`, o seguinte acontece:

1. O `BeerController` recebe a solicitação e chama `beerService.createBeer()`.
2. O `BeerService` constrói a requisição HTTP com WebClient, retornando um `Mono<String>` sem executá-lo.
3. O controller mapeia o resultado para um `ResponseEntity` e retorna o Mono.
4. O Spring WebFlux inscreve o Mono automaticamente, disparando a requisição HTTP.
5. Quando a resposta chega, o `doOnNext` no serviço registra o resultado no log, e o controller envia a resposta ao cliente.

## Boas Práticas para Fluxos Reativos

- **Evite `.subscribe()` em Camadas Intermediárias**: Deixe o controle para o controller.
- **Use Operadores para Logging**: `doOnNext`, `doOnError` etc.
- **Centralize a Configuração do WebClient**: Reutilização e organização.
- **Teste o Fluxo Reativo**: Use `StepVerifier` do Project Reactor.

## Indo Além: Robustez no WebClient

```java
return webClient.post()
    .uri("/beer")
    .bodyValue(beer)
    .exchangeToMono(response -> Mono.just("tudo certo!"))
    .timeout(Duration.ofSeconds(5))
    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
    .doOnNext(result -> log.info("Resultado: {}", result))
    .onErrorResume(e -> Mono.just("Erro: " + e.getMessage()));
```

## Conclusão

Neste artigo, construímos um fluxo reativo completo com Spring WebFlux, desde um controller REST até uma chamada HTTP com WebClient. O Spring gerencia a execução automaticamente e operadores como `doOnNext` garantem logging eficiente. No próximo artigo, veremos como integrar esse fluxo com o Kafka.

