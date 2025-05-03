
# Idempotência em Sistemas Distribuídos: Garantindo Processamento Único com Kafka

Nos artigos anteriores, construímos um fluxo reativo completo com Spring WebFlux e Reactor Kafka, desde o consumo e produção de mensagens até o envio de objetos complexos com resiliência. No entanto, em sistemas distribuídos, um desafio crítico permanece: garantir que mensagens sejam processadas exatamente uma vez, mesmo em cenários de falhas, retries ou duplicações. Este artigo explora o conceito de idempotência no contexto do Apache Kafka, mostrando como habilitá-lo no produtor e implementar estratégias no consumidor para evitar processamentos duplicados. Vamos mergulhar nesse tópico essencial para sistemas robustos!

## O Que é Idempotência e Por Que Ela Importa?

Em sistemas distribuídos, mensagens podem ser enviadas ou processadas múltiplas vezes devido a:

- **Retries automáticos**: Um produtor tenta reenviar uma mensagem após uma falha temporária.
- **Falhas de commit**: Um consumidor processa uma mensagem, mas falha antes de confirmar o offset, levando ao reprocessamento.
- **Duplicações acidentais**: Bugs ou configurações incorretas podem gerar mensagens idênticas.

Idempotência garante que, independentemente de quantas vezes uma operação seja executada, o resultado final seja o mesmo, como se ela tivesse ocorrido apenas uma vez.

No contexto do Kafka, isso significa:

- **No produtor**: Evitar que mensagens duplicadas sejam gravadas no tópico.
- **No consumidor**: Garantir que o processamento de uma mensagem não cause efeitos colaterais duplicados (ex.: criar dois pedidos para o mesmo evento).

Sem idempotência, você pode enfrentar problemas como registros duplicados em bancos de dados, pagamentos processados várias vezes, ou inconsistências no estado da aplicação.

## Passo 1: Habilitando Idempotência no Produtor Kafka

O Kafka oferece suporte nativo à idempotência no produtor, garantindo que mensagens duplicadas não sejam gravadas no tópico, mesmo em caso de retries. Vamos configurar isso no `KafkaSender` do Reactor Kafka.

### Atualizando o `application.yml`

```yaml
kafka:
  bootstrap-servers: localhost:9092
  producer:
    key-serializer: org.apache.kafka.common.serialization.StringSerializer
    value-serializer: com.example.kafka.BeerJsonSerializer
    topic: beer-topic
    enable-idempotence: true
```

### Atualizando a Configuração do `KafkaSender`

```java
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public SenderOptions<String, Beer> senderOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProducer().getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProperties.getProducer().getValueSerializer());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return SenderOptions.<String, Beer>create(props);
    }

    @Bean
    public KafkaSender<String, Beer> kafkaSender(SenderOptions<String, Beer> senderOptions) {
        return KafkaSender.create(senderOptions);
    }
}
```

### Como Funciona

- `enable-idempotence=true`: Ativa a idempotência no produtor, fazendo com que o Kafka atribua um ID único ao produtor e um número de sequência a cada mensagem.
- **Deduplicação no Broker**: O broker Kafka detecta e descarta mensagens duplicadas com base no ID do produtor e no número de sequência.

### Requisitos

- O produtor deve usar o mesmo `client.id`.
- O número máximo de partições por transação é limitado a 5.
- A configuração `acks` é automaticamente definida como `all`.

## Passo 2: Garantindo Idempotência no Consumidor

Embora a idempotência no produtor evite mensagens duplicadas no tópico, o consumidor ainda pode processar a mesma mensagem várias vezes.

### Definindo um Evento com ID Único

```java
import lombok.Data;

@Data
public class BeerEvent {
    private String eventId;
    private Beer beer;
}
```

### Configurando o Redis

**Dependência**:

```groovy
implementation 'org.springframework.boot:spring-boot-starter-data-redis'
```

**application.yml**:

```yaml
spring:
  redis:
    host: localhost
    port: 6379
```

### Atualizando o Consumidor Reativo

```java
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import javax.annotation.PostConstruct;
import java.time.Duration;

@Component
@RequiredArgsConstructor
public class BeerKafkaConsumer {

    private final ReceiverOptions<String, String> receiverOptions;
    private final BeerService beerService;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    private final Logger log = LoggerFactory.getLogger(BeerKafkaConsumer.class);

    @PostConstruct
    public void consume() {
        KafkaReceiver.create(receiverOptions)
            .receive()
            .flatMap(record -> {
                try {
                    BeerEvent event = objectMapper.readValue(record.value(), BeerEvent.class);
                    return processEvent(event)
                        .then(Mono.just(record));
                } catch (Exception e) {
                    log.error("Erro ao desserializar mensagem", e);
                    return Mono.just(record);
                }
            })
            .subscribe(record -> record.receiverOffset().acknowledge());
    }

    private Mono<Void> processEvent(BeerEvent event) {
        return redisTemplate.hasKey(event.getEventId())
            .flatMap(alreadyProcessed -> {
                if (alreadyProcessed) {
                    log.info("Evento {} já processado, ignorando", event.getEventId());
                    return Mono.empty();
                }
                return beerService.createBeer()
                    .doOnNext(response -> log.info("Resposta WebClient: {}", response))
                    .then(redisTemplate.opsForValue()
                        .set(event.getEventId(), "processed", Duration.ofHours(24)));
            });
    }
}
```

## Alternativas ao Redis

- Banco de dados com `INSERT ... ON CONFLICT DO NOTHING`
- Upserts no serviço externo
- Marcação no próprio Kafka

## Benefícios da Idempotência

- **Processamento Único**
- **Robustez**
- **Escalabilidade**
- **Flexibilidade**

## Boas Práticas

- Sempre use IDs únicos
- Escolha um armazenamento eficiente
- Defina TTLs razoáveis
- Teste cenários de falha
- Monitore duplicatas

## Indo Além

- Use transações Kafka (`exactly-once semantics`)
- Endpoints externos também devem ser idempotentes
- Monitore performance

## Conclusão

A idempotência é um pilar fundamental para sistemas distribuídos confiáveis. Neste artigo, habilitamos a idempotência no produtor Kafka com `enable-idempotence=true` e implementamos deduplicação no consumidor usando Redis, garantindo processamento único de eventos.
