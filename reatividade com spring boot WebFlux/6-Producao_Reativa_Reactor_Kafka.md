
# Produção Reativa de Mensagens com Reactor Kafka: Enviando Dados ao Kafka

Nos artigos anteriores, construímos fluxos reativos com Spring WebFlux, configuramos um consumidor reativo com Reactor Kafka, e externalizamos configurações no `application.yml`. Agora, é hora de completar o ciclo da mensageria explorando a produção reativa de mensagens com Reactor Kafka.

Neste artigo, vamos configurar um produtor reativo usando `KafkaSender`, enviar mensagens para o tópico `beer-topic`, e integrá-lo com o paradigma não-bloqueante do Spring WebFlux. Vamos mergulhar no código e aprender como enviar dados ao Kafka de forma eficiente e escalável!

## Por Que Produzir Mensagens de Forma Reativa?

O Reactor Kafka oferece uma abordagem reativa para produzir mensagens, alinhada com o Spring WebFlux. Em comparação com o `KafkaTemplate` do Spring Kafka, que opera em um modelo imperativo (com callbacks ou bloqueios), o `KafkaSender` do Reactor Kafka proporciona:

- **Não-Bloqueante:** Envio de mensagens sem ocupar threads, usando o event loop do Reactor.
- **Backpressure:** Controle automático do ritmo de produção, garantindo que o produtor não sobrecarregue o Kafka.
- **Integração com Mono/Flux:** Fluxos reativos que se conectam perfeitamente com outros componentes WebFlux.
- **Flexibilidade:** Suporte a retries, tratamento de erros e envio em lote de forma declarativa.

Nosso objetivo é criar um serviço que envie mensagens para o Kafka de forma reativa, acionado por um endpoint REST, e configurar tudo usando as propriedades externalizadas do `application.yml`.

## Passo 1: Configurando as Dependências

Certifique-se de que as dependências do Reactor Kafka e do Spring WebFlux estão no seu projeto. No `build.gradle`, inclua:

```groovy
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-webflux'
    implementation 'org.apache.kafka:kafka-clients:3.6.0' // ou versão mais recente
    implementation 'io.projectreactor.kafka:reactor-kafka:1.3.21' // ou versão mais recente
}
```

## Passo 2: Atualizando o application.yml

```yaml
kafka:
  bootstrap-servers: localhost:9092
  producer:
    key-serializer: org.apache.kafka.common.serialization.StringSerializer
    value-serializer: org.apache.kafka.common.serialization.StringSerializer
    topic: beer-topic
```

## Passo 3: Atualizando a Classe de Propriedades

```java
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kafka")
@Data
public class KafkaProperties {
    private String bootstrapServers;
    private Consumer consumer;
    private Producer producer;

    @Data
    public static class Consumer {
        private String groupId;
        private String autoOffsetReset;
        private String keyDeserializer;
        private String valueDeserializer;
        private String topic;
    }

    @Data
    public static class Producer {
        private String keySerializer;
        private String valueSerializer;
        private String topic;
    }
}
```

## Passo 4: Configurando o KafkaSender

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
    public SenderOptions<String, String> senderOptions() {
        Map<String, Object> props = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProducer().getKeySerializer(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProducer().getValueSerializer()
        );
        return SenderOptions.<String, String>create(props);
    }

    @Bean
    public KafkaSender<String, String> kafkaSender(SenderOptions<String, String> senderOptions) {
        return KafkaSender.create(senderOptions);
    }
}
```

## Passo 5: Criando o Serviço de Produção

```java
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
@RequiredArgsConstructor
public class BeerProducerService {
    private final KafkaSender<String, String> kafkaSender;
    private final KafkaProperties kafkaProperties;
    private final Logger log = LoggerFactory.getLogger(BeerProducerService.class);

    public Mono<Void> sendMessage(String key, String value) {
        SenderRecord<String, String, String> record = SenderRecord.create(
            new ProducerRecord<>(kafkaProperties.getProducer().getTopic(), key, value),
            key // correlation metadata
        );

        return kafkaSender.send(Mono.just(record))
                .doOnNext(result -> {
                    var metadata = result.recordMetadata();
                    log.info("Mensagem enviada para topic-partition {}-{} com offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/kafka")
public class KafkaProducerController {
    private final BeerProducerService beerProducerService;

    @PostMapping("/send")
    public Mono<ResponseEntity<String>> send(@RequestParam String key, @RequestParam String value) {
        return beerProducerService.sendMessage(key, value)
                .thenReturn(ResponseEntity.ok("Mensagem enviada com sucesso!"));
    }
}
```

## Benefícios do KafkaSender

- **Não-Bloqueante**
- **Backpressure**
- **Integração com WebFlux**
- **Flexibilidade**

## Boas Práticas para Produção Reativa

```java
.doOnError(e -> log.error("Erro ao enviar mensagem", e))
```

## Indo Além: Robustez no Produtor

```java
.retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
```

## Conclusão

Configuramos um produtor reativo com `KafkaSender`, integrando-o ao Spring WebFlux para enviar mensagens ao Kafka de forma não-bloqueante. No próximo artigo, abordaremos envio de objetos complexos em JSON e práticas de resiliência.

---
