/**
 * kafka-producer.js
 * ------------------
 * "Metodo custom" reutilizavel para publicar mensagens em um topico Kafka a partir
 * de qualquer request Bruno (via script:pre-request).
 *
 * Este arquivo fica versionado/commitado no Git normalmente - nao contem segredos.
 * Os valores sensiveis (senhas de truststore/keystore/SASL) vem de variaveis de
 * ambiente do Bruno marcadas como "secret" (ver environments/*.bru).
 *
 * Requer Bruno rodando em "Developer Mode" (icone do escudo no canto superior
 * direito -> Developer Mode), pois so nesse modo o require() de pacotes npm
 * funciona dentro dos scripts. Rode `npm install` uma vez dentro desta collection.
 */

const fs = require('fs');
const https = require('https');
const { Kafka, logLevel } = require('kafkajs');
const jks = require('jks-js');
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
const { requestJson } = require('./http-json');

const JKS_EXTENSIONS = /\.(jks|p12|pfx|pkcs12)$/i;
const PEM_EXTENSIONS = /\.(pem|crt|cer|key)$/i;

/**
 * Le um truststore ou keystore em disco e devolve o material em PEM
 * pronto para o `ssl` do kafkajs ({ ca, cert, key }).
 *
 * Aceita tanto arquivos Java KeyStore/PKCS12 (.jks, .p12, .pfx) quanto
 * arquivos ja em PEM (.pem, .crt, .cer, .key), detectando pela extensao.
 *
 * @param {string} filePath caminho absoluto (ou relativo ao cwd da collection) do arquivo
 * @param {string} password senha do store (obrigatoria para .jks/.p12/.pfx)
 * @param {'truststore'|'keystore'} kind so usado para mensagens de erro mais claras
 */
function loadStore(filePath, password, kind) {
  if (!filePath) return null;

  if (!fs.existsSync(filePath)) {
    throw new Error(`Kafka SSL: arquivo de ${kind} nao encontrado em "${filePath}".`);
  }

  // Arquivo ja em PEM: nao precisa de senha nem de parsing de JKS.
  if (PEM_EXTENSIONS.test(filePath)) {
    const content = fs.readFileSync(filePath, 'utf-8');
    return kind === 'truststore' ? { ca: [content] } : { cert: content, key: content };
  }

  if (!JKS_EXTENSIONS.test(filePath)) {
    throw new Error(
      `Kafka SSL: extensao nao reconhecida para ${kind} ("${filePath}"). ` +
      'Use .jks, .p12, .pfx (Java KeyStore) ou .pem/.crt/.cer/.key.'
    );
  }

  if (!password) {
    throw new Error(
      `Kafka SSL: e obrigatorio informar a senha do ${kind} para abrir "${filePath}".`
    );
  }

  const buffer = fs.readFileSync(filePath);
  let entries;
  try {
    entries = jks.toPem(buffer, password);
  } catch (err) {
    throw new Error(`Kafka SSL: falha ao ler ${kind} ("${filePath}"): ${err.message}`);
  }

  const aliases = Object.keys(entries);
  if (aliases.length === 0) {
    throw new Error(`Kafka SSL: nenhuma entrada encontrada no ${kind} ("${filePath}").`);
  }

  // Um truststore normalmente tem 1+ entradas "ca". Um keystore normalmente
  // tem 1 entrada com "cert" + "key" (a identidade do client para mTLS).
  const ca = [];
  let cert;
  let key;
  for (const alias of aliases) {
    const entry = entries[alias];
    if (entry.ca) ca.push(entry.ca);
    if (entry.cert) cert = entry.cert;
    if (entry.key) key = entry.key;
  }

  return { ca, cert, key };
}

/**
 * Monta o objeto `ssl` do kafkajs a partir da config informada.
 * Truststore e keystore sao OPCIONAIS e independentes:
 *   - so truststore  -> valida o certificado do broker (TLS "de um lado")
 *   - so keystore     -> raro, mas possivel (nao valida o broker)
 *   - ambos           -> mTLS completo (o caso mais comum em ambientes corporativos)
 *   - nenhum          -> TLS "solto", so com rejectUnauthorized
 */
function buildSslConfig(ssl) {
  if (!ssl || !ssl.enabled) return undefined;

  const sslConfig = {
    rejectUnauthorized: ssl.rejectUnauthorized !== false,
  };

  if (ssl.truststore && ssl.truststore.path) {
    const trust = loadStore(ssl.truststore.path, ssl.truststore.password, 'truststore');
    if (trust && trust.ca && trust.ca.length) {
      sslConfig.ca = trust.ca;
    }
  }

  if (ssl.keystore && ssl.keystore.path) {
    const identity = loadStore(ssl.keystore.path, ssl.keystore.password, 'keystore');
    if (identity && identity.cert) sslConfig.cert = identity.cert;
    if (identity && identity.key) sslConfig.key = identity.key;
  }

  return sslConfig;
}

/**
 * Cria o client do Schema Registry.
 *
 * Detalhe que costuma queimar tempo em ambiente corporativo: se o registry esta
 * atras de HTTPS com CA interna, o client do Confluent usa o trust store padrao
 * do Node e falha com "self signed certificate in chain". Aqui reaproveitamos o
 * MESMO truststore/keystore ja configurado para o Kafka, entao quem configurou o
 * broker nao precisa configurar nada extra para o registry.
 *
 * @param {object} schemaRegistry { url, auth? }
 * @param {object} [sslConfig]    resultado de buildSslConfig() - opcional
 */
function createRegistry(schemaRegistry, sslConfig) {
  if (!schemaRegistry || !schemaRegistry.url) {
    throw new Error('Schema Registry: "url" e obrigatorio quando schemaRegistry.enabled=true.');
  }

  let agent;
  if (/^https:/i.test(schemaRegistry.url) && sslConfig) {
    agent = new https.Agent({
      ca: sslConfig.ca,
      cert: sslConfig.cert,
      key: sslConfig.key,
      rejectUnauthorized: sslConfig.rejectUnauthorized !== false,
    });
  }

  const auth = (schemaRegistry.auth && schemaRegistry.auth.username)
    ? { username: schemaRegistry.auth.username, password: schemaRegistry.auth.password }
    : undefined;

  const registry = new SchemaRegistry({ host: schemaRegistry.url, auth, agent });

  // Guardamos url/auth/agent junto porque o registro de schema e feito por HTTP
  // direto (ver registerSchema), sem passar pelo register() da biblioteca.
  registry.__http = { url: String(schemaRegistry.url).replace(/\/+$/, ''), auth, agent };
  return registry;
}

const SCHEMA_TYPE_MAP = { AVRO: SchemaType.AVRO, JSON: SchemaType.JSON, PROTOBUF: SchemaType.PROTOBUF };

/**
 * Registra um schema no subject e devolve o id.
 *
 * Por que nao usamos o `registry.register()` da biblioteca: alem do POST do
 * schema, ele faz um `GET /config/{subject}` e falha se a compatibilidade do
 * subject nao for BACKWARD, e ainda faz um `PUT /config/{subject}` na primeira
 * vez - ou seja, uma ferramenta de TESTE acabaria alterando a politica de
 * compatibilidade do registry da empresa (quando o usuario tem permissao; quando
 * nao tem, quebra com 403 sem relacao nenhuma com o payload).
 *
 * Aqui fazemos so o POST que interessa. Se um schema identico ja existir no
 * subject, o proprio registry devolve o id existente em vez de criar uma versao
 * nova.
 */
async function registerSchema(registry, subject, schema, typeName) {
  const type = (typeName || 'AVRO').toUpperCase();
  if (!SCHEMA_TYPE_MAP[type]) {
    throw new Error(`Schema Registry: tipo de schema desconhecido "${typeName}". Use AVRO, JSON ou PROTOBUF.`);
  }
  if (type !== 'PROTOBUF') {
    try {
      JSON.parse(schema);
    } catch (err) {
      throw new Error(
        `Schema Registry: o schema ${type} informado nao e um JSON valido (${err.message}). ` +
        'Se voce colou em kafkaSchemaInline, cole o JSON em uma linha so.'
      );
    }
  }

  const { url, auth, agent } = registry.__http;
  const response = await requestJson(`${url}/subjects/${encodeURIComponent(subject)}/versions`, {
    method: 'POST',
    contentType: 'application/vnd.schemaregistry.v1+json',
    auth,
    agent,
    body: { schema, schemaType: type === 'AVRO' ? undefined : type },
  });

  if (!response || typeof response.id !== 'number') {
    throw new Error(`Schema Registry: resposta inesperada ao registrar em "${subject}": ${JSON.stringify(response)}`);
  }
  return response.id;
}

/**
 * Resolve o schema id a usar para codificar key/value, na seguinte ordem de
 * prioridade:
 *   1. `entry.id`      -> usa esse id direto (mais rapido, zero chamadas HTTP)
 *   2. `entry.schema`   -> registra (ou reaproveita, se identico) esse schema
 *                          no subject e usa o id retornado
 *   3. `entry.subject` (ou o default `${topic}-value` / `${topic}-key`)
 *                       -> busca o id do ULTIMO schema publicado nesse subject
 *                          (o caso mais comum: o schema ja foi registrado por
 *                          quem escreveu o servico produtor)
 *
 * @param {import('@kafkajs/confluent-schema-registry').SchemaRegistry} registry
 * @param {object} entry            { id?, schema?, type?, subject? }
 * @param {string} defaultSubject   usado quando entry.subject nao e informado
 */
async function resolveSchemaId(registry, entry, defaultSubject) {
  if (entry.id !== undefined && entry.id !== null) {
    return entry.id;
  }

  const subject = entry.subject || defaultSubject;

  if (entry.schema) {
    return registerSchema(registry, subject, entry.schema, entry.type);
  }

  if (!subject) {
    throw new Error(
      'Schema Registry: informe "id", "schema" ou "subject" para resolver o schema a usar.'
    );
  }

  return registry.getLatestSchemaId(subject);
}

/**
 * Codifica um payload no formato wire do Confluent Schema Registry
 * (magic byte + schema id + payload serializado) usando o schema resolvido
 * por `resolveSchemaId`.
 *
 * @returns {Promise<{id:number, buffer:Buffer}>} o id usado (util para exibir
 *          no resultado) e o Buffer pronto para virar `key`/`value`.
 */
async function encodeWithSchemaRegistry(registry, entry, defaultSubject, payload) {
  const id = await resolveSchemaId(registry, entry, defaultSubject);
  return { id, buffer: await registry.encode(id, payload) };
}

/**
 * Publica UMA mensagem em um topico Kafka.
 *
 * @param {object} opts
 * @param {string[]} opts.brokers          lista de brokers "host:porta"
 * @param {string} [opts.clientId]
 * @param {string} opts.topic
 * @param {string|Buffer} [opts.key]       chave da mensagem (ignorado se schemaRegistry.key for usado)
 * @param {string|object} opts.value       valor da mensagem (objetos viram JSON, ou sao
 *                                          codificados via Schema Registry se schemaRegistry estiver ativo)
 * @param {Object.<string,string>} [opts.headers]
 * @param {number} [opts.partition]
 * @param {number} [opts.acks]             default -1 (todas as replicas)
 * @param {number} [opts.timeout]          default 30000ms
 * @param {object} [opts.ssl]              { enabled, rejectUnauthorized, truststore:{path,password}, keystore:{path,password} }
 * @param {object} [opts.sasl]             { enabled, mechanism, username, password }
 * @param {object} [opts.schemaRegistry]   { enabled, url, auth:{username,password}?,
 *                                            value:{ id?, schema?, type?, subject? },
 *                                            key?:{ id?, schema?, type?, subject? } }
 *                                          value/key sao OPCIONAIS entre si: sem `schemaRegistry.key`,
 *                                          a key continua indo como string normal.
 * @param {boolean} [opts.dryRun]          codifica/valida contra o schema mas NAO publica
 * @returns {Promise<{topic:string, schemaId?:number, bytes:number, dryRun:boolean, records:object[]}>}
 */
async function sendKafkaMessage(opts) {
  const {
    brokers, clientId, topic, key, value, headers,
    partition, acks, timeout, ssl, sasl, schemaRegistry, dryRun,
  } = opts || {};

  if (!brokers || !brokers.length) throw new Error('Kafka: "brokers" e obrigatorio.');
  if (!topic) throw new Error('Kafka: "topic" e obrigatorio.');
  if (value === undefined) {
    throw new Error('Kafka: "value" e obrigatorio (use null para publicar um tombstone).');
  }

  // value === null e um TOMBSTONE: mensagem com valor nulo, usada para apagar
  // uma chave em topico compactado. Nao passa pelo Schema Registry.
  const isTombstone = value === null;

  const sslConfig = buildSslConfig(ssl);
  let encodedValue;
  let encodedKey; // so definido quando schemaRegistry.key for usado
  let schemaId;

  if (!isTombstone && schemaRegistry && schemaRegistry.enabled) {
    const registry = createRegistry(schemaRegistry, sslConfig);

    const encoded = await encodeWithSchemaRegistry(
      registry, schemaRegistry.value || {}, `${topic}-value`, value
    );
    encodedValue = encoded.buffer;
    schemaId = encoded.id;

    if (schemaRegistry.key) {
      encodedKey = (await encodeWithSchemaRegistry(
        registry, schemaRegistry.key, `${topic}-key`, key
      )).buffer;
    }
  } else if (isTombstone) {
    encodedValue = null;
  } else {
    encodedValue = typeof value === 'string' ? value : JSON.stringify(value);
  }

  const bytes = encodedValue === null ? 0 : Buffer.byteLength(encodedValue);

  // Dry run: tudo foi montado e (se houver registry) o payload ja foi VALIDADO
  // contra o schema. So nao abrimos conexao com o broker.
  if (dryRun) {
    return { topic, schemaId, bytes, dryRun: true, records: [] };
  }

  const kafka = new Kafka({
    clientId: clientId || 'bruno-kafka-producer',
    brokers,
    ssl: sslConfig,
    sasl: (sasl && sasl.enabled) ? {
      mechanism: sasl.mechanism || 'plain',
      username: sasl.username,
      password: sasl.password,
    } : undefined,
    logLevel: logLevel.NOTHING,
  });

  const producer = kafka.producer();
  await producer.connect();

  try {
    const records = await producer.send({
      topic,
      acks: acks === undefined ? -1 : acks,
      timeout: timeout === undefined ? 30000 : timeout,
      messages: [{
        key: encodedKey !== undefined
          ? encodedKey
          : (key === undefined || key === null ? undefined : String(key)),
        value: encodedValue,
        headers,
        partition,
      }],
    });

    return { topic, schemaId, bytes, dryRun: false, records };
  } finally {
    await producer.disconnect();
  }
}

module.exports = {
  sendKafkaMessage,
  buildSslConfig,
  createRegistry,
  registerSchema,
  loadStore,
  resolveSchemaId,
  encodeWithSchemaRegistry,
};
