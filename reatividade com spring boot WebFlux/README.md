1. "Entendendo o Papel do Subscribe no Spring WebFlux: Por Que Sua Requisição Não Executa?"
Conteúdo: Explica por que o subscribe() é necessário para disparar operações reativas no Spring WebFlux, usando o exemplo do WebClient. Aborda o conceito de "lazy evaluation" em Mono e Flux, os impactos no ciclo de threads (não-bloqueante) e os riscos de usar subscribe() sem cuidado. Inclui dicas sobre quando evitar o subscribe() e como deixar o Spring gerenciar a execução.

2. "Construindo um Fluxo Reativo Completo com Spring WebFlux e WebClient"
Conteúdo: Apresenta um exemplo prático de um fluxo reativo completo, desde o Controller até o Service, usando WebClient para fazer chamadas HTTP. Mostra como o Spring WebFlux gerencia a execução sem a necessidade de subscribe() manual, com ênfase em logging com doOnNext. Inclui o código do modelo Beer e boas práticas para manter a reatividade.

3. "Integrando Kafka com Spring WebFlux: Comportamento em Listeners Imperativos"
Conteúdo: Explora o comportamento de chamar um serviço reativo (como o BeerService) a partir de um @KafkaListener imperativo. Discute por que o Mono não é executado sem subscribe() ou block(), os prós e contras de cada abordagem e como evitar bloqueios em aplicações reativas. Introduz a ideia de consumidores reativos como alternativa.

4. "Consumo Reativo de Kafka com Reactor Kafka: Um Guia Prático"
Conteúdo: Demonstra como configurar um consumidor reativo usando Reactor Kafka, incluindo a configuração do KafkaReceiver e integração com o WebClient. Explica os benefícios de manter o fluxo não-bloqueante e como lidar com mensagens do Kafka de forma reativa. Inclui o código do BeerKafkaConsumer e configurações básicas.

5. "Externalizando Configurações do Kafka com Spring Boot e application.yml"
Conteúdo: Mostra como configurar o Kafka (consumidor e produtor) usando o application.yml para evitar valores fixos no código. Apresenta o uso de @ConfigurationProperties para organizar propriedades em uma classe como KafkaProperties. Discute a importância de configurações dinâmicas para diferentes ambientes.

6. "Produção Reativa de Mensagens com Reactor Kafka: Enviando Dados ao Kafka"
Conteúdo: Detalha como configurar um produtor reativo com KafkaSender para enviar mensagens ao Kafka. Inclui um exemplo de envio de mensagens simples e explica como o Reactor Kafka usa a biblioteca kafka-clients nos bastidores. Compara com abordagens imperativas e destaca a integração com WebFlux.

7. "Enviando Objetos Complexos e Garantindo Resiliência com Reactor Kafka"
Conteúdo: Avança no uso do KafkaSender, mostrando como enviar objetos complexos (como Beer) serializados em JSON. Apresenta a criação de um serializador personalizado (BeerJsonSerializer) e adiciona resiliência com retries e fallback usando operadores do Reactor. Discute boas práticas para lidar com falhas.

8. "Idempotência em Sistemas Distribuídos: Garantindo Processamento Único com Kafka"
Conteúdo: Explica o conceito de idempotência no contexto de produtores e consumidores Kafka. Mostra como habilitar idempotência no KafkaSender e estratégias para garantir processamento único no consumidor (ex.: uso de Redis para deduplicação). Inclui exemplos práticos e discute cenários comuns de duplicação.


