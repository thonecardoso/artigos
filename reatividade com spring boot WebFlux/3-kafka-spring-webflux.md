
# Integrando Kafka com Spring WebFlux: Comportamento em Listeners Imperativos

Nos artigos anteriores, exploramos o papel do `subscribe()` no Spring WebFlux e como construir um fluxo reativo completo com `WebClient`. Agora, vamos abordar um cenário comum em aplicações modernas: integrar o Spring WebFlux com o Apache Kafka. Especificamente, veremos o que acontece quando um listener imperativo do Kafka, anotado com `@KafkaListener`, chama um serviço reativo que retorna um `Mono`. Este artigo explica por que o comportamento pode não ser o esperado, as opções para lidar com isso, e como evitar armadilhas que comprometem a reatividade. Vamos mergulhar!

## O Contexto: Kafka e WebFlux Juntos

O Apache Kafka é amplamente usado para processamento de eventos em arquiteturas distribuídas, enquanto o Spring WebFlux é ideal para construir APIs reativas e não-bloqueantes. Um caso comum é ter um consumidor Kafka que escuta mensagens em um tópico e, ao receber uma mensagem, chama um serviço reativo para realizar uma ação, como uma requisição HTTP via `WebClient`.

Imagine que você tem o serviço `BeerService` do artigo anterior, que faz uma chamada `POST` para criar um recurso Beer e retorna um `Mono<String>`:

```java
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
                .doOnNext(result -> log.info("Resultado: {}", result));
    }
}
```

Agora, suponha que você tem um listener Kafka que escuta mensagens no tópico `beer-topic` e tenta chamar esse serviço:

```java
@Component
public class BeerKafkaListener {
    private final BeerService beerService;

    @KafkaListener(topics = "beer-topic")
    public void listen(String message) {
        beerService.createBeer();
    }
}
```

Você espera que a requisição HTTP seja feita e o resultado apareça nos logs, mas... nada acontece. Por quê?

## O Problema: Listeners Imperativos e Fluxos Reativos

O `@KafkaListener`, fornecido pelo Spring Kafka, opera em um modelo imperativo. Ele executa o método `listen` de forma síncrona em uma thread do consumidor Kafka (geralmente uma por partição). Por outro lado, o `BeerService` retorna um `Mono`, que é reativo e preguiçoso (*lazy*). Como aprendemos no primeiro artigo, um `Mono` só executa sua lógica quando alguém se "inscreve" nele com `.subscribe()` ou quando ele é consumido por outra parte do fluxo reativo.

No exemplo acima, o método `createBeer()` retorna um `Mono<String>`, mas o listener não faz nada com ele. O resultado? A pipeline é criada, mas nunca executada. A requisição HTTP não é enviada, e o log não aparece.

## Opções para Resolver o Problema

Para fazer o serviço reativo funcionar dentro de um listener imperativo, você tem algumas opções, cada uma com seus prós e contras. Vamos explorá-las.

### Opção 1: Usar `.subscribe()` no Listener

A solução mais direta é chamar `.subscribe()` no `Mono` retornado pelo serviço:

```java
@KafkaListener(topics = "beer-topic")
public void listen(String message) {
    beerService.createBeer()
        .subscribe();
}
```

#### Comportamento

- O `.subscribe()` dispara a execução do `Mono`, enviando a requisição HTTP.
- A execução é assíncrona e não-bloqueante.
- O método `listen` termina imediatamente, sem esperar o resultado da requisição.
- O `WebClient` processa a requisição em uma thread do event loop (geralmente Netty).

#### Prós

- Simples e direto.
- Mantém a reatividade do `WebClient`, sem bloquear a thread do Kafka.
- Ideal para cenários *fire-and-forget*.

#### Contras

- Perda de controle: não há retorno do resultado, a menos que se use callbacks.
- Difícil de testar por ser um efeito colateral.
- Pode gerar concorrência descontrolada com muitos `.subscribe()` simultâneos.

### Opção 2: Usar `.block()` para Execução Síncrona

Outra abordagem é usar `.block()` para forçar a execução síncrona do `Mono`:

```java
@KafkaListener(topics = "beer-topic")
public void listen(String message) {
    String result = beerService.createBeer().block();
    log.info("Resultado: {}", result);
}
```

#### Comportamento

- O `.block()` faz a thread do Kafka esperar até que a requisição HTTP complete.

#### Prós

- Comportamento previsível.
- Útil quando o resultado influencia decisões no listener.

#### Contras

- Bloqueia a thread do Kafka, o que pode causar gargalos.
- Reduz a escalabilidade.
- Contraria o paradigma reativo do `WebFlux`.

> **Aviso:** O uso de `.block()` deve ser evitado em aplicações WebFlux, exceto em casos muito específicos.

### Opção 3: Controlar a Execução com `Schedulers`

Para manter a reatividade e evitar concorrência descontrolada, você pode usar `.subscribeOn()`:

```java
@KafkaListener(topics = "beer-topic")
public void listen(String message) {
    beerService.createBeer()
        .subscribeOn(Schedulers.boundedElastic())
        .subscribe(result -> log.info("Resultado async: {}", result));
}
```

#### Comportamento

- A execução ocorre em uma thread separada do pool `boundedElastic`.

#### Prós

- Mantém a reatividade e evita bloqueio.
- Permite tratamento de erros com callbacks.

#### Contras

- Ainda é efeito colateral.
- Exige cuidado com o uso do pool `boundedElastic`.

## A Melhor Alternativa: Consumidores Reativos

Embora as opções acima funcionem, são soluções de compromisso. A abordagem mais alinhada com o `Spring WebFlux` é usar um consumidor reativo do Kafka, como o fornecido pelo **Reactor Kafka**.

Com ele, é possível consumir mensagens com `KafkaReceiver`, encadeando a lógica com operadores como `flatMap`. Vantagens:

- **Não-bloqueante de ponta a ponta**
- **Backpressure**
- **Integração natural com `WebFlux`**

## Boas Práticas para Integração Kafka-WebFlux

- Prefira `.subscribe()` a `.block()`.
- Use `Schedulers` com cautela.
- Considere consumidores reativos para máxima reatividade.
- Teste o comportamento assíncrono com ferramentas como `StepVerifier`.

## Conclusão

Integrar o `Spring WebFlux` com o Kafka pode ser desafiador com listeners imperativos. O comportamento lazy dos tipos reativos exige decisões explícitas. Embora `.subscribe()` e `.block()` funcionem, a melhor solução é adotar consumidores reativos.

Até lá, experimente as abordagens acima e observe como o comportamento do seu listener Kafka muda. Você está pronto para dar o próximo passo rumo à reatividade total?
