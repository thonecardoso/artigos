
# Enviando Objetos Complexos e Garantindo Resiliência com Reactor Kafka

Nos artigos anteriores, configuramos consumidores e produtores reativos com Reactor Kafka, externalizamos configurações no `application.yml`, e integramos tudo com o Spring WebFlux. Até agora, nossas mensagens foram strings simples, mas em aplicações reais, frequentemente precisamos enviar objetos complexos, como o modelo `Beer`, serializados em formatos como JSON.

Além disso, sistemas distribuídos exigem resiliência para lidar com falhas, como erros temporários de rede ou indisponibilidade do Kafka. Neste artigo, vamos aprender a enviar objetos complexos com um serializador personalizado e garantir resiliência com retries e fallbacks usando o Reactor Kafka. Vamos ao código!

## Por Que Enviar Objetos Complexos e Ser Resiliente?

Em arquiteturas baseadas em eventos, é comum enviar dados estruturados, como um pedido, um evento de usuário ou, no nosso caso, um objeto `Beer`. Serializar esses objetos em JSON é uma prática padrão, pois JSON é amplamente suportado e fácil de processar.

No entanto, enviar mensagens ao Kafka pode falhar devido a:

- **Falhas de Rede**: Conexões instáveis com o broker Kafka.
- **Indisponibilidade Temporária**: O cluster Kafka pode estar sobrecarregado ou em manutenção.
- **Erros de Serialização**: Problemas ao converter objetos em bytes.

Para lidar com isso, o Reactor Kafka oferece ferramentas poderosas, como operadores de `retry` e `fallback`, que combinam com o paradigma reativo do Spring WebFlux.

Nosso objetivo é:

- Enviar um objeto `Beer` serializado em JSON.
- Configurar um serializador personalizado.
- Adicionar retries para falhas temporárias.
- Implementar um fallback para falhas irrecuperáveis.

## Passo 1: Reutilizando o Modelo Beer

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

## Passo 2: Criando um Serializador Personalizado

```java
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class BeerJsonSerializer implements Serializer<Beer> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Beer data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Erro ao serializar objeto Beer", e);
        }
    }
}
```

## Passo 3: Atualizando o application.yml

```yaml
kafka:
  bootstrap-servers: localhost:9092
  producer:
    key-serializer: org.apache.kafka.common.serialization.StringSerializer
    value-serializer: com.example.kafka.BeerJsonSerializer
    topic: beer-topic
```

## Passo 4: Atualizando a Configuração do KafkaSender

```java
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public SenderOptions<String, Beer> senderOptions() {
        Map<String, Object> props = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProducer().getKeySerializer(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProducer().getValueSerializer()
        );

        return SenderOptions.<String, Beer>create(props);
    }

    @Bean
    public KafkaSender<String, Beer> kafkaSender(SenderOptions<String, Beer> senderOptions) {
        return KafkaSender.create(senderOptions);
    }
}
```

## Passo 5: Atualizando o Serviço de Produção

```java
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class BeerProducerService {

    private final KafkaSender<String, Beer> kafkaSender;
    private final KafkaProperties kafkaProperties;
    private final Logger log = LoggerFactory.getLogger(BeerProducerService.class);

    public Mono<Void> sendBeer(String key, Beer beer) {
        SenderRecord<String, Beer, String> record = SenderRecord.create(
            new ProducerRecord<>(kafkaProperties.getProducer().getTopic(), key, beer),
            key
        );

        return kafkaSender.send(Mono.just(record))
                .doOnNext(result -> {
                    var metadata = result.recordMetadata();
                    log.info("Beer enviado para topic {} com offset {}", metadata.topic(), metadata.offset());
                })
                .doOnError(e -> log.error("Erro ao enviar Beer", e))
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(2))
                    .filter(throwable -> !(throwable instanceof SerializationException)))
                .onErrorResume(e -> {
                    log.warn("Fallback: salvando beer localmente ou em outro sistema");
                    return Mono.empty();
                })
                .then();
    }
}
```

## Passo 6: Criando um Controller para Testar

```java
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/kafka")
public class KafkaProducerController {

    private final BeerProducerService beerProducerService;

    @PostMapping("/send/beer")
    public Mono<ResponseEntity<String>> sendBeer(@RequestBody Beer beer) {
        return beerProducerService.sendBeer(beer.getUpc(), beer)
                .thenReturn(ResponseEntity.ok("Beer enviado!"));
    }
}
```

## Benefícios da Abordagem

- Envio de Dados Complexos
- Resiliência com retries e fallback
- Fluxo reativo com WebFlux
- Serializador personalizável

## Boas Práticas para Produção Resiliente

- Valide objetos antes do envio.
- Filtre erros no `retry`.
- Implemente fallbacks significativos.
- Teste com Embedded Kafka.

## Indo Além

- Use timeout com `.timeout(Duration.ofSeconds(5))`
- Habilite `enable.idempotence=true`
- Envie objetos em lote com `Flux<SenderRecord>`

## Conclusão

Aprendemos a enviar objetos complexos como `Beer` serializados em JSON com um serializador personalizado, adicionando resiliência com `retry` e `fallback`, de forma reativa com o Reactor Kafka.

No próximo artigo, exploraremos a idempotência em sistemas distribuídos.
