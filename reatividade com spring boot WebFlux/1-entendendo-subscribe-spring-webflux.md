
# Entendendo o Papel do Subscribe no Spring WebFlux: Por Que Sua Requisição Não Executa?

O Spring WebFlux trouxe uma revolução para o desenvolvimento de aplicações Java, permitindo criar sistemas reativos que lidam com operações assíncronas e não-bloqueantes de forma eficiente. No entanto, para quem está começando, um dos conceitos mais desafiadores é entender por que certas operações, como uma chamada HTTP com WebClient, simplesmente não executam sem o uso de `.subscribe()`. Neste artigo, vamos explorar o papel do `subscribe()` no ciclo de vida de uma operação reativa, seus impactos no gerenciamento de threads e as práticas recomendadas para evitar armadilhas comuns.

## O Problema: Minha Requisição Não Aparece no Log!

Imagine que você está desenvolvendo uma aplicação que faz uma requisição POST para criar um recurso, como um objeto `Beer`, usando o WebClient do Spring WebFlux. Seu código pode parecer algo assim:

```java
webClient
    .post()
    .uri("/beer")
    .bodyValue(Beer.builder()
        .id(UUID.randomUUID())
        .version(1)
        .beerName("Create")
        .beerStyle(BeerStyle.PALE_ALE)
        .upc("12356222")
        .price(new BigDecimal("11.99"))
        .quantityOnHand(392)
        .createdDate(LocalDateTime.now())
        .updateDate(LocalDateTime.now())
        .build())
    .exchangeToMono(c -> {
        if (c.statusCode().is2xxSuccessful()) {
            return Mono.just("tudo certo! " + c.statusCode().value());
        } else {
            return Mono.just("something went wrong! " + c.statusCode().value());
        }
    })
    .doOnNext(log::info);
```

Você espera que o resultado da requisição apareça nos logs, mas... nada acontece. Ao adicionar `.subscribe()` no final da cadeia, como abaixo, o log finalmente aparece:

```java
webClient
    .post()
    ...
    .doOnNext(log::info)
    .subscribe();
```

Por que isso acontece? A resposta está no coração da programação reativa: operações como `Mono` e `Flux` são preguiçosas (lazy).

## Lazy Evaluation: O Segredo do Comportamento Reativo

No Spring WebFlux, `Mono` e `Flux` são tipos reativos que representam, respectivamente, zero ou um resultado (`Mono`) e zero ou mais resultados (`Flux`). Esses tipos não executam nenhuma operação até que sejam explicitamente "ativados". Em termos técnicos, isso é chamado de *lazy evaluation*.

Quando você encadeia operações como `.post()`, `.bodyValue()`, ou `.exchangeToMono()`, você está apenas construindo uma pipeline de processamento. Essa pipeline define o que fazer, mas não dispara a execução. O `.subscribe()` é o gatilho que diz: "Execute a pipeline agora!".

Sem o `.subscribe()`, a requisição HTTP nunca é enviada, o servidor não é contatado, e o log não é registrado. Isso é intencional no paradigma reativo, pois permite maior controle sobre quando e como as operações são realizadas, além de otimizar recursos ao evitar execuções desnecessárias.

## O Papel do Subscribe no Ciclo de Threads

Uma preocupação comum é: "O uso de `.subscribe()` impacta o desempenho ou o ciclo de threads da minha aplicação?". Vamos esclarecer.

Quando você chama `.subscribe()`, a execução da pipeline começa, e o Spring WebFlux utiliza o modelo de *event loop* (geralmente baseado no Netty) para processar a requisição de forma não-bloqueante. Isso significa que:

- Nenhuma thread fica "presa" esperando a resposta da requisição HTTP. O processamento ocorre em threads do *event loop*, que são altamente eficientes para operações I/O.
- O resultado é processado assincronamente quando disponível, geralmente em uma thread diferente da que iniciou o `.subscribe()`.

Por exemplo, no código acima, o `.doOnNext(log::info)` será executado em uma thread do *event loop* quando a resposta HTTP chegar, sem bloquear a thread principal da sua aplicação. Isso é uma vantagem significativa em comparação com APIs bloqueantes, como o `RestTemplate`, que mantém threads ocupadas durante toda a operação.

## Subscribe e Desempenho

O uso de `.subscribe()` não implica perda de desempenho por si só. Na verdade, ele é essencial para disparar operações reativas. No entanto, há algumas ressalvas importantes:

### Efeitos Colaterais Incontrolados

Se você chamar `.subscribe()` diretamente em um serviço ou em um trecho de código imperativo, pode acionar múltiplas execuções acidentais ou perder o controle sobre o fluxo. Por exemplo:

```java
beerService.createBeer().subscribe(); // Dispara, mas você não controla o resultado
beerService.createBeer().subscribe(); // Dispara novamente, talvez duplicando a ação
```

Isso pode levar a comportamentos inesperados, como chamadas duplicadas ao servidor.

### Dificuldade em Testes e Debugging

Usar `.subscribe()` diretamente torna mais difícil testar o código, pois você não retorna um `Mono` ou `Flux` que possa ser manipulado em testes. Além disso, erros podem ser engolidos se não forem tratados adequadamente.

### Concorrência Mal Gerenciada

Em cenários onde `.subscribe()` é usado em loops ou em múltiplos pontos, você pode acabar sobrecarregando o sistema com execuções paralelas sem controle.

## Práticas Recomendadas: Quando e Como Usar o Subscribe

Para evitar esses problemas, aqui estão algumas práticas recomendadas:

### 1. Evite `.subscribe()` em Camadas de Serviço

Em vez de chamar `.subscribe()` dentro de um serviço, retorne o `Mono` ou `Flux` para a camada superior (geralmente um controller). O Spring WebFlux gerencia a execução automaticamente quando o resultado é necessário. Por exemplo:

```java
@Service
public class BeerService {
    private final WebClient webClient;
    private final Logger log = LoggerFactory.getLogger(BeerService.class);

    public Mono<String> createBeer() {
        return webClient.post()
            .uri("/beer")
            .bodyValue(Beer.builder().build())
            .exchangeToMono(response -> Mono.just("tudo certo! " + response.statusCode().value()))
            .doOnNext(result -> log.info("Resultado: {}", result));
    }
}
```

```java
@RestController
@RequestMapping("/api/beers")
public class BeerController {
    private final BeerService beerService;

    @PostMapping("/create")
    public Mono<ResponseEntity<String>> createBeer() {
        return beerService.createBeer()
            .map(response -> ResponseEntity.ok(response));
    }
}
```

### 2. Use `.subscribe()` para Efeitos Colaterais Controlados

Se você realmente precisa disparar uma operação como um efeito colateral (ex.: logging, envio de métricas, ou uma tarefa assíncrona), use `.subscribe()` com cuidado. Certifique-se de tratar erros adequadamente:

```java
beerService.createBeer()
    .subscribe(
        result -> log.info("Sucesso: {}", result),
        error -> log.error("Erro: {}", error.getMessage())
    );
```

### 3. Considere Operadores Reativos para Logging

Para logging, o operador `.doOnNext()` é ideal, pois permite registrar informações sem interferir no fluxo reativo. Combine-o com um retorno de `Mono` ou `Flux` para manter a reatividade:

```java
webClient.post()
    .uri("/beer")
    .exchangeToMono(response -> Mono.just("tudo certo!"))
    .doOnNext(response -> log.info("Resposta: {}", response));
```

### 4. Evite `.block()` Sempre que Possível

Embora seja tentador usar `.block()` para forçar a execução síncrona (ex.: `beerService.createBeer().block()`), isso quebra o paradigma reativo e pode causar gargalos em aplicações WebFlux. Reserve `.block()` para casos excepcionais, como integração com sistemas legados.

## Conclusão

O método `.subscribe()` é o gatilho que dá vida às operações reativas no Spring WebFlux, mas seu uso exige cuidado para evitar armadilhas como execuções duplicadas ou dificuldades em testes. Ao entender o conceito de *lazy evaluation* e seguir práticas como retornar `Mono`/`Flux` em serviços e deixar o Spring gerenciar a execução, você pode aproveitar ao máximo a eficiência do modelo reativo.

No próximo artigo, veremos como construir um fluxo reativo completo com WebClient, desde o controller até o serviço, eliminando a necessidade de `.subscribe()` manual e mantendo a aplicação não-bloqueante. Até lá, experimente os exemplos acima e observe como o Spring WebFlux transforma a forma como lidamos com requisições assíncronas!
