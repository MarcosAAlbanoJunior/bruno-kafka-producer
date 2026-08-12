# Kafka Producer - Bruno

Collection Bruno com um **método custom reutilizável** para publicar mensagens em um
tópico Kafka, com suporte a TLS/mTLS via truststore e keystore (Java KeyStore .jks/.p12
ou PEM), ambos **opcionais** e cada um com sua própria senha, e suporte **opcional** a
Confluent Schema Registry (Avro, JSON Schema ou Protobuf).

## Estrutura

```
kafka-bruno/
├── bruno.json                  # descritor da collection
├── package.json                # deps: kafkajs + jks-js + @kafkajs/confluent-schema-registry (commitado)
├── .gitignore                  # ignora node_modules e .env
├── .env.sample                 # template das senhas (commitado, SEM valores)
├── .env                        # senhas reais (você cria, NUNCA commitar)
├── lib/
│   └── kafka-producer.js       # o "método custom" -> sendKafkaMessage()
├── schemas/
│   └── pedido-value.avsc       # exemplo de schema Avro versionado no Git (aberto)
├── scripts/
│   ├── register-avro-schema.sh # curl (via jq) para registrar schema no Schema Registry
│   └── produce-via-rest-proxy.sh # curl para publicar via Kafka REST Proxy
├── environments/
│   └── local.bru               # config aberta: brokers, tópico, caminhos dos stores
└── Send Kafka Message.bru      # request que aciona o método
```

**O que é aberto/commitável:** `bruno.json`, `package.json`, `lib/kafka-producer.js`,
`environments/*.bru`, `Send Kafka Message.bru`, `.env.sample`, `README.md` — nada aqui
tem segredo, pode subir pro GitHub sem medo.

**O que NUNCA vai pro Git:** o arquivo `.env` (já está no `.gitignore`) — é ele que tem
as senhas de verdade do truststore/keystore/SASL.

## Setup

### 1. Descompacte e abra a pasta no Bruno

- Extraia o zip em algum lugar fixo (idealmente dentro do repositório Git onde isso vai
  morar — não dentro da pasta de instalação do Bruno, que é só o executável do app).
- Abra o Bruno. Na tela inicial, clique em **"Open Collection"** (ou `File > Open
  Collection` no menu superior) e selecione a pasta `kafka-bruno` (a pasta que contém o
  `bruno.json`, não um arquivo específico).
- A collection deve aparecer na barra lateral esquerda, com o request **"Send Kafka
  Message"** dentro dela.

### 2. Instale as dependências (kafkajs e jks-js)

Isso é feito fora do Bruno, num terminal:

- Abra o **Prompt de Comando**, **PowerShell** ou **Windows Terminal**.
- Verifique se tem Node.js instalado: `node -v` e `npm -v`. Se der erro de comando não
  reconhecido, instale o Node.js primeiro (nodejs.org, versão LTS) e reabra o terminal.
- Navegue até a pasta da collection, por exemplo:
  ```
  cd C:\Users\marco\Documents\kafka-bruno
  ```
- Rode:
  ```
  npm install
  ```
  Isso cria a pasta `node_modules` com `kafkajs` e `jks-js` — é ela que o Bruno vai
  usar quando o script fizer `require(...)`. **Sem esse passo o script quebra** com erro
  do tipo `Cannot find module 'kafkajs'`.

### 3. Ative o Developer Mode no Bruno

- No canto **superior direito** da janela do Bruno tem um ícone de **escudo** (shield).
- Clique nele: abre um menu com duas opções, **Safe Mode** e **Developer Mode**.
- Selecione **Developer Mode**. O Bruno pode mostrar um aviso dizendo que scripts vão
  poder acessar o sistema de arquivos/executar comandos — confirme.
- Isso é **obrigatório**: no Safe Mode (padrão), `require()` de pacotes npm dentro dos
  scripts é bloqueado e o request nem chega a tentar falar com o Kafka.
- Esse toggle é por instalação do Bruno, então cada pessoa/máquina que for rodar essa
  collection precisa ativar o Developer Mode uma vez.

### 4. Configure as senhas (arquivo `.env`)

- Dentro da pasta `kafka-bruno`, faça uma cópia do arquivo `.env.sample` e renomeie
  para `.env` (pelo Explorer: copiar, colar, renomear; ou no terminal:
  `copy .env.sample .env`).
- Abra o `.env` com o Bloco de Notas (ou seu editor) e preencha só o que for usar:
  ```
  KAFKA_TRUSTSTORE_PASSWORD=senha-do-truststore
  KAFKA_KEYSTORE_PASSWORD=senha-do-keystore
  KAFKA_SASL_PASSWORD=senha-do-usuario-sasl
  ```
- Salve. Esse arquivo **não** vai pro Git (já está listado no `.gitignore`) — é só
  local, na sua máquina.

### 5. Configure o environment (brokers, tópico, caminho dos certificados)

- No Bruno, perto do canto superior direito (ao lado do botão **Send**) tem um seletor
  de environment — provavelmente escrito **"No Environment"** ou já **"local"**.
- Clique nele e escolha **local**. Se quiser editar os valores, clique no ícone de
  engrenagem/lápis ao lado do seletor (ou "Configure") para abrir a tela de variáveis
  do environment.
- Ajuste o que precisar:
  - `kafkaBrokers` — lista separada por vírgula, ex: `broker1:9092,broker2:9092`
  - `kafkaTopic` — nome do tópico
  - `kafkaSslEnabled` — `true` se o cluster exigir TLS, senão `false`
  - `kafkaTruststorePath` / `kafkaKeystorePath` — caminho completo do arquivo
    (`.jks`, `.p12` ou `.pem`/`.crt`/`.key`). **Deixe em branco se não for usar** — os
    dois são opcionais e independentes
  - `kafkaSaslEnabled`, `kafkaSaslMechanism`, `kafkaSaslUsername` — só se o cluster
    também (ou em vez disso) usar SASL_SSL
- Para ter environments separados (`dev`, `staging`, `prod`), clique nos "..." do
  environment `local` na lista e escolha **Clone**, renomeie e ajuste os valores de
  cada um. As senhas continuam vindo do `.env` — não duplicam.

### 6. Envie a mensagem

- Abra o request **"Send Kafka Message"** na barra lateral.
- Vá na aba **Body**: o JSON que estiver lá é o valor que será publicado no tópico.
  Edite como quiser.
- Confira no canto superior direito se o environment certo (`local`, `dev`, etc.) está
  selecionado.
- Clique em **Send**.
- Confira o resultado:
  - Aba **Response**: mostra o eco com `kafka.status: "success"` (ou `"error"` com a
    mensagem, se algo falhou).
  - Aba **Tests**: deve aparecer um ✅ verde em "Mensagem publicada no Kafka com
    sucesso".
  - Console/DevTools do Bruno (`Ctrl+Shift+I` ou menu **View > Toggle DevTools**):
    mostra o `console.log`/`console.error` com o retorno completo do `kafkajs`
    (partição, offset etc.) ou o erro detalhado.

### Erros comuns

| Erro | Causa provável |
|---|---|
| `require is not defined` ou script falha sem detalhe | Developer Mode não está ativado (passo 3) |
| `Cannot find module 'kafkajs'` | `npm install` não foi rodado na pasta certa (passo 2) |
| `e obrigatorio informar a senha do truststore/keystore` | Caminho preenchido no environment mas senha vazia no `.env` |
| `ECONNREFUSED` / `Connection error` | Broker errado/inacessível em `kafkaBrokers`, ou faltou VPN/rede até o cluster |
| Erro de certificado/handshake TLS | Senha errada do truststore/keystore, ou arquivo `.jks` corrompido/alias inesperado |
| `Subject not found` / erro 404 do Schema Registry | `kafkaValueSchemaSubject` (ou o default `<topic>-value`) não existe ainda nesse registry — publique um schema primeiro (via `kafkaValueSchemaFile`) ou confira o nome do subject |
| `informe "id", "schema" ou "subject"` | Nenhuma das três formas de resolver o schema foi preenchida (passo Schema Registry) |

## Schema Registry (Avro / JSON Schema / Protobuf)

Se o cluster usa Confluent Schema Registry, ative no environment:

- `kafkaSchemaRegistryEnabled: true`
- `kafkaSchemaRegistryUrl`: ex. `http://localhost:8081` (ou `https://...`, se o
  registry estiver atrás de TLS)
- `kafkaSchemaRegistryUsername` — **opcional**, só se o registry exigir basic auth.
  A senha vai no `.env` (`KAFKA_SCHEMA_REGISTRY_PASSWORD`), nunca no environment aberto.
- `kafkaValueSchemaType`: `AVRO` (padrão), `JSON` ou `PROTOBUF`.

E aí escolha **uma** destas três formas de indicar qual schema usar (nessa ordem de
prioridade caso mais de uma esteja preenchida):

1. **`kafkaValueSchemaId`** — você já sabe o id numérico do schema no registry (o
   jeito mais rápido: nenhuma chamada extra ao registry além do encode).
2. **`kafkaValueSchemaFile`** — caminho (relativo à raiz da collection) de um arquivo
   `.avsc`/`.json` **versionado no Git**, ex. `schemas/pedido-value.avsc` (já incluso
   como exemplo). O script registra esse schema no subject na hora de enviar — se o
   conteúdo já existir lá, o registry devolve o id existente (não duplica).
3. **Nenhum dos dois acima** — o script busca o **último schema publicado** no subject
   (por padrão `<kafkaTopic>-value`, o padrão do Confluent; dá pra sobrescrever com
   `kafkaValueSchemaSubject` se o seu subject tiver outro nome).

Com Schema Registry ativado, o **JSON da aba Body continua sendo o valor da mensagem**
— só que agora ele é validado contra o schema e serializado no formato wire do Confluent
(magic byte + schema id + Avro/Protobuf/JSON binário) em vez de virar JSON puro. Não
precisa mudar nada no fluxo de edição do request.

Isso tudo mora em `lib/kafka-producer.js` também (`sendKafkaMessage({ schemaRegistry: {...} })`),
então funciona exatamente com o mesmo "método custom" — é só mais uma opção dele. Se
você também quiser a **key** Avro-codificada (menos comum — a maioria usa key como
string simples), dá pra passar `schemaRegistry.key` com a mesma forma de `schemaRegistry.value`;
isso não está exposto como variável de environment por padrão, mas é só editar o
`script:pre-request` do request e adicionar o bloco `key: {...}` igual ao `value`.

## Como funciona o "método custom"

Toda a lógica de conectar no Kafka, montar a config de SSL (lendo truststore/keystore,
convertendo `.jks`/`.p12` para PEM automaticamente) e publicar a mensagem está em
`lib/kafka-producer.js`, na função `sendKafkaMessage(opts)`. Qualquer request `.bru`
pode importar e usar essa função no `script:pre-request`:

```js
const { sendKafkaMessage } = require(path.join(bru.cwd(), 'lib', 'kafka-producer.js'));

await sendKafkaMessage({
  brokers: ['broker1:9092'],
  topic: 'meu-topico',
  value: { hello: 'world' },      // objeto ou string
  key: 'chave-opcional',
  ssl: {
    enabled: true,
    truststore: { path: 'C:/certs/truststore.jks', password: '...' }, // opcional
    keystore:  { path: 'C:/certs/keystore.jks',  password: '...' },   // opcional
  },
  sasl: { enabled: false },
});
```

Truststore e keystore são tratados de forma independente: você pode informar só um dos
dois, os dois, ou nenhum. Se o **caminho** for informado, a **senha correspondente passa
a ser obrigatória** (o script lança um erro claro se faltar). Arquivos `.jks`/`.p12`/`.pfx`
são convertidos para PEM automaticamente via `jks-js`; arquivos já em `.pem`/`.crt`/`.key`
são lidos diretamente, sem senha.

## Por que o request dispara uma chamada HTTP também?

O Bruno sempre executa a requisição HTTP configurada quando você clica em **Send** fora
do Collection Runner — não existe um tipo de request "só script". Por isso o request
aponta para um echo (`https://httpbin.org/post`, configurável em `kafkaEchoUrl`) só para
ecoar o resultado do Kafka na aba Response e facilitar visualizar o que aconteceu. O
trabalho de verdade (conectar/publicar no Kafka) já aconteceu no `script:pre-request`,
antes dessa chamada sair.

Se você rodar essa collection via **Bruno CLI** (`bru run`) em um pipeline, aí sim dá pra
chamar `bru.runner.skipRequest()` no fim do script para pular a chamada HTTP de vez,
porque o CLI sempre executa através do motor do Runner.

## Múltiplos tópicos / múltiplos requests

Basta duplicar `Send Kafka Message.bru`, trocar `kafkaTopic` (por variável de request,
por exemplo `bru.setVar('kafkaTopic', 'outro-topico')` antes de chamar
`sendKafkaMessage`) e o body. A lógica de conexão/SSL/SASL fica toda centralizada em
`lib/kafka-producer.js` — você não duplica nada disso.

## Publicando via curl (sem abrir o Bruno)

`curl` fala HTTP — o protocolo nativo do Kafka é binário sobre TCP, então não dá pra
"curlar" direto num broker. O Schema Registry, porém, **é** uma API REST comum, e dá pra
publicar mensagens via curl **se** houver um REST Proxy (Confluent REST Proxy, Karapace
REST, Kafka Bridge...) na frente do cluster. Sem REST Proxy, a collection do Bruno
continua sendo o caminho (ela fala o protocolo do Kafka nativamente via `kafkajs`, sem
precisar dessa camada extra).

Dois scripts em `scripts/` cobrem os dois pedaços, e ambos foram testados de ponta a
ponta contra mocks locais das duas APIs antes de entrar no repo:

**1. Registrar um schema (a partir de um arquivo `.avsc`/`.json`) no Schema Registry:**

```bash
./scripts/register-avro-schema.sh http://localhost:8081 pedidos-value schemas/pedido-value.avsc
# com auth: ./scripts/register-avro-schema.sh https://registry:8081 pedidos-value schemas/pedido-value.avsc AVRO usuario:senha
```

Por baixo, isso é um curl assim (o script só cuida de escapar o conteúdo do arquivo
certinho via `jq`, que é a parte chata de fazer na mão):

```bash
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$(jq -n --rawfile schema schemas/pedido-value.avsc '{schema: $schema, schemaType: "AVRO"}')" \
  http://localhost:8081/subjects/pedidos-value/versions
```

A resposta traz o `id` do schema — é esse id que você usa em `kafkaValueSchemaId` no
Bruno, ou no passo 2 abaixo.

**2. Publicar uma mensagem no tópico via REST Proxy, usando esse schema id:**

```bash
./scripts/produce-via-rest-proxy.sh http://localhost:8082 pedidos 1 '{"orderId":"12345","status":"created"}'
# ou lendo o valor de um arquivo:
./scripts/produce-via-rest-proxy.sh http://localhost:8082 pedidos 1 @payload.json
```

Curl equivalente:

```bash
curl -X POST -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  -H "Accept: application/vnd.kafka.v2+json" \
  --data '{"value_schema_id": 1, "records": [{"value": {"orderId":"12345","status":"created"}}]}' \
  http://localhost:8082/topics/pedidos
```

Ambos os scripts pedem `curl` e `jq` (só isso, nada de Node/Bruno) e trazem `--help`
implícito: rode sem argumentos pra ver a mensagem de uso.
