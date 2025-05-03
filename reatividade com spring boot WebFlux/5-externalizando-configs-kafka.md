
# Externalizando Configurações do Kafka com Spring Boot e `application.yml`

Nos artigos anteriores, construímos um fluxo reativo completo com Spring WebFlux, integramos o Kafka usando listeners imperativos e, em seguida, migramos para um consumidor reativo com Reactor Kafka. Até agora, nossas configurações do Kafka, como o endereço do broker e o grupo de consumidores, foram definidas diretamente no código. Embora isso funcione para exemplos simples, não é ideal para aplicações em produção, onde flexibilidade e manutenção são cruciais.

Neste artigo, vamos explorar como externalizar as configurações do Kafka usando o arquivo `application.yml` do Spring Boot e a anotação `@ConfigurationProperties`, tornando sua aplicação mais configurável e adaptável a diferentes ambientes.

---

## Por Que Externalizar Configurações?

Em aplicações reais, você frequentemente precisa ajustar configurações como o endereço do broker Kafka, o ID do grupo de consumidores ou o tópico com base no ambiente (desenvolvimento, teste, produção). Hardcoding dessas propriedades no código tem várias desvantagens:

- **Manutenção Difícil**: Alterar configurações exige recompilar e redeployar a aplicação.
- **Risco de Erros**: Valores fixos podem ser esquecidos ou mal configurados em diferentes ambientes.
- **Falta de Flexibilidade**: Ambientes como Kubernetes ou pipelines CI/CD dependem de configurações externas.

O Spring Boot resolve isso com o arquivo `application.yml`, que centraliza configurações e permite sobrescrevê-las via variáveis de ambiente, argumentos de linha de comando ou perfis (ex.: `application-prod.yml`). Combinado com `@ConfigurationProperties`, podemos mapear essas configurações para objetos Java de forma limpa e tipada.

---

## Passo 1: Definindo o `application.yml`

```yaml
kafka:
  bootstrap-servers: localhost:9092
  consumer:
    group-id: beer-consumer-group
    auto-offset-reset: earliest
    key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    topic: beer-topic
  producer:
    key-serializer: org.apache.kafka.common.serialization.StringSerializer
    value-serializer: org.apache.kafka.common.serialization.StringSerializer
    topic: beer-topic
```

### Explicação

- **Hierarquia**: A seção `kafka` agrupa todas as propriedades relacionadas ao Kafka.
- **Propriedades do Consumidor**: `group-id`, `auto-offset-reset`, `key-deserializer`, `value-deserializer`, `topic`.
- **Propriedades do Produtor**: `key-serializer`, `value-serializer`, `topic`.
- **Bootstrap Servers**: O endereço do cluster Kafka, compartilhado entre consumidor e produtor.

---

## Passo 2: Criando uma Classe de Propriedades com `@ConfigurationProperties`

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

### Dependências no `build.gradle`

```groovy
compileOnly 'org.projectlombok:lombok:1.18.30'
annotationProcessor 'org.projectlombok:lombok:1.18.30'
annotationProcessor 'org.springframework.boot:spring-boot-configuration-processor'
```

---

## Passo 3: Atualizando a Configuração do Kafka

```java
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public ReceiverOptions<String, String> receiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer().getGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getConsumer().getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getConsumer().getValueDeserializer());

        return ReceiverOptions.<String, String>create(props)
                .subscription(Collections.singleton(kafkaProperties.getConsumer().getTopic()));
    }
}
```

---

## Passo 4: Benefícios da Externalização

- **Flexibilidade**
- **Manutenção Simplificada**
- **Suporte a Perfis**
- **Integração com CI/CD**

```yaml
# application-prod.yml
kafka:
  bootstrap-servers: prod-kafka:9092
  consumer:
    group-id: beer-consumer-group-prod
    topic: beer-topic-prod
```

E ativar o perfil com:

```properties
spring.profiles.active=prod
```

---

## Boas Práticas

- Use `@ConfigurationProperties` para manter o código limpo.
- Valide propriedades com `@NotNull`, `@NotEmpty`.
- Documente seu `application.yml` com comentários.
- Teste com `@ActiveProfiles`.
- Use Vault ou variáveis de ambiente para credenciais.

---

## Indo Além: Configurações Avançadas

```yaml
kafka:
  consumer:
    max-poll-records: 100
    security-protocol: SASL_SSL
```

---

## Conclusão

Externalizar as configurações do Kafka com `application.yml` e `@ConfigurationProperties` é uma prática essencial para criar aplicações robustas e adaptáveis.

No próximo artigo, abordaremos a **produção reativa de mensagens com o Reactor Kafka**, mostrando como enviar mensagens para o Kafka de forma não-bloqueante.
