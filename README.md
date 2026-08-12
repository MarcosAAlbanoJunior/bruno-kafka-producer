# Kafka no Bruno

Collection Bruno para **publicar e conferir mensagens Kafka** com o mesmo conforto
de disparar um endpoint HTTP: escolhe o ambiente, abre o request, edita o JSON,
roda. Suporta TLS/mTLS (truststore e keystore `.jks`/`.p12`/`.pem`), SASL e
Confluent Schema Registry (Avro, JSON Schema, Protobuf) — todos opcionais.

## O modelo mental (é só isto)

| Onde | O que mora ali | Com que frequência muda |
|---|---|---|
| **Environment** | o **cluster**: brokers, certificados, senhas, registry | 3x (local, dev, hml) |
| **Pasta** | a **aplicação**: qual certificado/usuário usar | 1x por app |
| **Request** | o **evento**: tópico, key, headers, payload | dezenas |

O tópico **não** fica no environment. É isso que evita a explosão de
"um environment por tópico" quando você tem várias aplicações e vários clusters.

```
kafka-bruno/
├── environments/          local.bru | dev.bru | hml.bru  <- 1 por CLUSTER
├── pagamentos/            <- 1 pasta por APLICAÇÃO
│   ├── Pedido Criado.bru
│   └── Pedido Cancelado.bru
├── Ferramentas/
│   ├── Doctor.bru                      diagnóstico em 1 clique
│   └── Consumir Ultimas Mensagens.bru  confere o que chegou
├── lib/                   toda a lógica (você não mexe no dia a dia)
├── schemas/               schemas Avro versionados (opcional)
├── scripts/               curl para Schema Registry / REST Proxy
└── tests/                 npm test
```

## Setup (uma vez por máquina)

1. **Abra a collection**: Bruno > *Open Collection* > selecione a pasta `kafka-bruno`.
2. **Instale as dependências**: no terminal, dentro da pasta, `npm install`.
   Sem isso o script quebra com `Cannot find module 'kafkajs'`.
3. **Ative o Developer Mode**: ícone de escudo no canto superior direito >
   *Developer Mode*. Obrigatório — no Safe Mode o `require()` de pacotes npm é
   bloqueado.
4. **Preencha as senhas na UI**: selecione o environment, clique na engrenagem ao
   lado do seletor, e preencha as variáveis marcadas como **secret**
   (`kafkaTruststorePassword`, `kafkaKeystorePassword`, `kafkaSaslPassword`,
   `kafkaSchemaRegistryPassword`). Elas ficam guardadas **fora** da collection —
   o `.bru` versionado só tem o *nome* delas, nunca o valor. Não existe `.env`.
5. **Aponte para os seus certificados**: ajuste `kafkaCertsDir` (o diretório onde
   os `.jks` estão *na sua máquina*). Os nomes dos arquivos já vêm no environment
   versionado e são iguais para todo mundo.
6. **Rode o Doctor** (`Ferramentas > Doctor`) para confirmar que está tudo de pé.

## O dia a dia

**Enviar uma mensagem:** selecione o environment → abra o request → edite o Body →
botão direito no request > **Run**.

> Use sempre o **Run/Runner**, não o botão Send. É no Runner que o script cancela
> a chamada HTTP falsa que o Bruno dispararia, deixando só o Kafka acontecer.
> (Se alguém clicar em Send por hábito, a URL padrão aponta para `127.0.0.1`, então
> nenhum payload sai da máquina — só aparece um erro de conexão inofensivo.)

**O que você edita em cada request:**

| Aba | Vira o quê |
|---|---|
| **Body** | o valor da mensagem. Aceita `{{variáveis}}` do Bruno. Vazio = tombstone |
| **Headers** | os **headers da mensagem Kafka** (`content-type` e afins são ignorados) |
| **Vars > Pre Request** | `kafkaTopic`, `kafkaKey`, `kafkaApp` |

**Onde ver o resultado:** Console do Bruno (`Ctrl+Shift+I` > Console) mostra o
recibo — partição, offset, schema id, tempo. A aba **Tests** fica verde. Em caso
de erro, a mensagem aparece direto no Runner, já com a causa provável.

```
----------------------------------------------------------------
 Kafka - produzir
 ambiente    hml   (aplicacao: pagamentos)
 topico      pagamentos.pedido.criado.v1   key: 12345
 schema      AVRO id=42 (pagamentos.pedido.criado.v1-value)
 headers     tipo-evento, origem
 [OK] publicado   particao 0   offset 1187   127 bytes   412ms
----------------------------------------------------------------
```

**Conferir se chegou:** `Ferramentas > Consumir Ultimas Mensagens` lê as últimas N
mensagens do tópico com o payload Avro já decodificado em JSON. Usa um consumer
group descartável, então **não mexe no offset dos consumers reais**. O resultado
completo também vai para `out/last-consume.json`.

## Como crescer

| Preciso de... | Faço |
|---|---|
| **novo tópico/evento** | botão direito num request > *Clone*, troco `kafkaTopic` e o Body |
| **nova aplicação** | nova pasta + requests com `kafkaApp: <nome>`; no environment, variáveis com sufixo `_<nome>` |
| **novo cluster/ambiente** | *Clone* de um environment, ajusto brokers/certs e preencho as senhas na UI |
| **outro keystore no mesmo cluster** | `kafkaKeystore_<app>` + `kafkaKeystorePassword_<app>` no environment |

O sufixo `_<app>` é o truque que evita duplicar environment: quem declara
`kafkaApp: pagamentos` recebe automaticamente `kafkaKeystore_pagamentos`,
`kafkaSaslUsername_pagamentos` etc., caindo na variável sem sufixo quando não
existir uma específica.

Nada disso mexe em script: os requests chamam uma linha só de
`lib/bruno-kafka.js`, onde toda a lógica mora.

## Referência de variáveis

**Environment (cluster).** Qualquer uma aceita o sufixo `_<kafkaApp>`.

| Variável | Para quê |
|---|---|
| `kafkaBrokers` | `host:porta` separados por vírgula |
| `kafkaClientId` | client id que aparece nos logs do cluster |
| `kafkaSslEnabled` / `kafkaSslRejectUnauthorized` | liga TLS / aceita cert inválido |
| `kafkaCertsDir` | diretório dos certificados **nesta máquina** |
| `kafkaTruststore` / `kafkaKeystore` | nome do arquivo (`.jks`, `.p12`, `.pem`...) |
| `kafkaTruststorePassword` / `kafkaKeystorePassword` | 🔒 secret, exigidas só para `.jks`/`.p12` |
| `kafkaSaslEnabled` / `kafkaSaslMechanism` / `kafkaSaslUsername` | SASL (`plain`, `scram-sha-256`, `scram-sha-512`) |
| `kafkaSaslPassword` | 🔒 secret |
| `kafkaSchemaRegistryEnabled` / `kafkaSchemaRegistryUrl` / `kafkaSchemaRegistryUsername` | Schema Registry |
| `kafkaSchemaRegistryPassword` | 🔒 secret |
| `kafkaIsProduction` / `kafkaAllowProduction` | rede de segurança contra produção (abaixo); não usadas nos environments atuais |
| `kafkaDryRun` | valida sem publicar |
| `kafkaEchoUrl` | URL "de fachada" do request; deixe apontando para `127.0.0.1` |

**Request/pasta (evento).**

| Variável | Para quê |
|---|---|
| `kafkaTopic` | tópico de destino (obrigatório) |
| `kafkaApp` | qual conjunto de variáveis `_<app>` usar |
| `kafkaKey` | key da mensagem (opcional) |
| `kafkaPartition` / `kafkaAcks` / `kafkaTimeoutMs` | controle fino do envio |
| `kafkaSchemaSubject` / `kafkaSchemaId` / `kafkaSchemaInline` / `kafkaSchemaFile` | como resolver o schema |
| `kafkaMaxMessages` / `kafkaFromBeginning` | só no request de consumir |

## Produção não mora aqui

Esta collection cobre **local, dev e hml**. Não existe environment de produção, e
publicar em produção não é caso de uso dela.

A trava continua no código como rede de segurança: se alguém clonar um
environment e chamar de `prod`/`prd`, ou marcar `kafkaIsProduction: true`, o envio
é **recusado** até que se defina explicitamente `kafkaAllowProduction: true`. Ou
seja, criar um ambiente de produção por engano não publica nada por engano.
`kafkaDryRun: true` continua liberado nesse caso, para validar payload sem enviar.

## Schema Registry

Ligue `kafkaSchemaRegistryEnabled` e informe a URL. O schema a usar é resolvido
nesta ordem:

1. `kafkaSchemaId` — você já sabe o id numérico (mais rápido).
2. `kafkaSchemaInline` — cola o schema direto na tela de Environment/Vars, em uma
   linha só. Registra no subject na hora; se um schema idêntico já existir, o
   registry devolve o id existente em vez de criar versão nova.
3. `kafkaSchemaFile` — caminho de um `.avsc`/`.json` versionado em `schemas/`.
4. **nada configurado** (o caso normal) — usa o **último schema publicado** no
   subject `<tópico>-value`, que é o padrão do Confluent.

O Body continua sendo o mesmo JSON: ele passa a ser validado contra o schema e
serializado no formato wire do Confluent. Se o payload não bate com o schema, o
erro aparece **antes** de qualquer coisa ser publicada.

O registro é feito com um `POST /subjects/.../versions` puro, de propósito: o
`register()` da biblioteca oficial também faz `GET`/`PUT /config/{subject}` e
acabaria **alterando a política de compatibilidade** do registry da empresa (ou
falhando com 403 sem relação com o seu payload).

Se o registry estiver atrás de HTTPS com CA interna, ele reaproveita
automaticamente o truststore já configurado para o Kafka — não precisa configurar
nada a mais.

## Quando der errado

Rode `Ferramentas > Doctor`. Ele testa em ordem — dependências, variáveis,
certificados (abre o JKS de verdade), conexão com o broker, existência do tópico,
Schema Registry e o subject — e devolve um checklist com a causa provável de cada
falha em vez de um stack trace.

| Sintoma | Causa provável |
|---|---|
| `require is not defined` / script morre sem detalhe | Developer Mode desligado |
| `Cannot find module 'kafkajs'` | faltou `npm install` na pasta da collection |
| `ECONNREFUSED` / timeout | broker errado, ou VPN desligada |
| `self signed certificate in chain` | a CA do servidor não está no truststore |
| erro de senha ao abrir o `.jks` | variável secreta vazia ou senha errada |
| `Subject not found` | o schema ainda não foi registrado nesse cluster |
| `PRODUCAO BLOQUEADA` | é proposital: veja *Trava de produção* |

## Publicando via curl (sem abrir o Bruno)

`curl` fala HTTP, e o protocolo do Kafka é binário sobre TCP — então só dá para
publicar por curl se houver um **REST Proxy** na frente do cluster. Os dois
scripts em `scripts/` cobrem isso (precisam de `curl` e `jq`; rode sem argumentos
para ver o uso):

```bash
# 1. registrar um schema
./scripts/register-avro-schema.sh http://localhost:8081 pedidos-value schemas/pagamentos.pedido.criado.v1-value.avsc

# 2. publicar usando o id devolvido acima
./scripts/produce-via-rest-proxy.sh http://localhost:8082 pedidos 1 '{"orderId":"12345","status":"created"}'
```

## Mexendo na lógica

Tudo mora em `lib/`, com testes: `npm test` (34 casos — resolução de variáveis,
trava de produção, tombstone, encode/decode Avro contra um Schema Registry
mockado, cálculo de offsets do consumidor e as mensagens de erro).

| Arquivo | Responsabilidade |
|---|---|
| `lib/bruno-kafka.js` | ponte Bruno↔Kafka: é o que os requests chamam |
| `lib/kafka-config.js` | variáveis do Bruno → configuração, com validação |
| `lib/kafka-producer.js` | conexão, TLS/JKS, Schema Registry, envio |
| `lib/kafka-consumer.js` | leitura das últimas N mensagens + decode |
| `lib/kafka-doctor.js` | diagnóstico |
| `lib/http-json.js` | cliente HTTP mínimo para o registry |

Também dá para usar `sendKafkaMessage()` fora do Bruno (num script Node ou no
`bru run` de um pipeline):

```js
const { sendKafkaMessage } = require('./lib/kafka-producer');

await sendKafkaMessage({
  brokers: ['broker1:9093'],
  topic: 'pagamentos.pedido.criado.v1',
  key: '12345',
  value: { orderId: '12345', status: 'created' },
  headers: { 'tipo-evento': 'PEDIDO_CRIADO' },
  ssl: {
    enabled: true,
    truststore: { path: 'C:/certs/truststore.jks', password: '...' },
    keystore: { path: 'C:/certs/app.jks', password: '...' },
  },
});
```
