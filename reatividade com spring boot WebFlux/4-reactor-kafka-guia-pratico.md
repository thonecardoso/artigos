
# Consumo Reativo de Kafka com Reactor Kafka: Um Guia Prático

Nos artigos anteriores, exploramos os fundamentos do Spring WebFlux, construímos um fluxo reativo com WebClient, e analisamos os desafios de integrar listeners imperativos do Kafka com serviços reativos. Agora, vamos elevar o nível e mergulhar no consumo reativo de mensagens Kafka usando o Reactor Kafka. Este artigo apresenta um guia prático para configurar um consumidor reativo, integrá-lo com o WebClient, e manter um fluxo 100% não-bloqueante. Vamos ver como o Reactor Kafka transforma a forma como processamos eventos em aplicações reativas!

## Por Que Consumir Kafka de Forma Reativa?

O Spring Kafka, com seu `@KafkaListener`, é excelente para cenários imperativos, mas, como vimos no artigo anterior, ele não se integra naturalmente com o paradigma reativo do Spring WebFlux. O `@KafkaListener` opera de forma síncrona, o que pode levar a bloqueios ou execuções incontroladas ao usar `Mono` ou `Flux`. O Reactor Kafka, por outro lado, foi projetado para trabalhar com o Project Reactor, oferecendo:

- **Processamento não-bloqueante**: Consumo de mensagens sem ocupar threads desnecessariamente.
- **Backpressure**: Controle automático do ritmo de consumo, evitando sobrecarga.
- **Integração com WebFlux**: Fluxos reativos que se conectam perfeitamente com `Mono` e `Flux`.
- **Escalabilidade**: Aproveita o event loop do Reactor para lidar com grandes volumes de mensagens.

## Passo 1: Configurando as Dependências

Para usar o Reactor Kafka, precisamos adicionar sua dependência ao projeto. Se você usa Gradle, inclua no `build.gradle`:

```groovy
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-webflux'
    implementation 'org.apache.kafka:kafka-clients:3.6.0' // ou versão mais recente
    implementation 'io.projectreactor.kafka:reactor-kafka:1.3.21' // ou versão mais recente
}
```

## Passo 2: Configurando o Consumidor Kafka

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ReceiverOptions<String, String> kafkaReceiverOptions() {
        Map<String, Object> props = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG, "beer-consumer-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        return ReceiverOptions.<String, String>create(props)
                .subscription(List.of("beer-topic"));
    }
}
```

## Passo 3: Implementando o Consumidor Reativo

```java
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import javax.annotation.PostConstruct;

@Component
@RequiredArgsConstructor
public class BeerKafkaConsumer {

    private final ReceiverOptions<String, String> receiverOptions;
    private final BeerService beerService;
    private final Logger log = LoggerFactory.getLogger(BeerKafkaConsumer.class);

    @PostConstruct
    public void consume() {
        KafkaReceiver.create(receiverOptions)
            .receive()
            .doOnNext(record -> log.info("Mensagem recebida: {}", record.value()))
            .flatMap(record -> beerService.createBeer()
                .doOnNext(response -> log.info("Resposta WebClient: {}", response))
            )
            .subscribe();
    }
}
```

## Passo 4: Configurando o `application.yml`

```yaml
kafka:
  bootstrap-servers: localhost:9092
```

## Passo 5: Reutilizando o BeerService

```java
@Service
@RequiredArgsConstructor
public class BeerService {
    private final WebClient webClient = WebClient.create("http://localhost:8081");
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
                });
    }
}
```

## Benefícios do Fluxo Reativo

- **Não-Bloqueante**: Tanto o consumo do Kafka quanto a requisição HTTP ocorrem no event loop do Reactor.
- **Backpressure**: Pausa o consumo se o processamento estiver lento.
- **Integração Fluida**: `flatMap` conecta o `Flux` do Kafka ao `Mono` do serviço.
- **Escalabilidade**: Lida com grandes volumes de mensagens eficientemente.

## Boas Práticas para Consumidores Reativos

- **Gerencie o Subscribe com Cuidado**: Trate erros com `doOnError` ou `onErrorResume`.
- **Monitore Backpressure**: Use `maxInFlight` no `ReceiverOptions`.
- **Teste com Embedded Kafka**: Utilize o `kafka-test`.
- **Externalize Configurações**: Use `application.yml`.

## Indo Além: Robustez e Escalabilidade

Exemplo com retry:

```java
.flatMap(record -> beerService.createBeer()
    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
    .doOnNext(response -> log.info("Resposta WebClient: {}", response))
)
```

## Conclusão

Configuramos um consumidor reativo com Reactor Kafka e o integramos ao Spring WebFlux. O resultado é um fluxo escalável, eficiente e alinhado com o paradigma reativo.

No próximo artigo, exploraremos como externalizar as configurações do Kafka no `application.yml` usando `@ConfigurationProperties`.
