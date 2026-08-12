/**
 * kafka-config.js
 * ---------------
 * Traduz as VARIAVEIS do Bruno (environment + request/pasta) na config que o
 * `kafka-producer.js` / `kafka-consumer.js` esperam.
 *
 * Modelo mental (o unico que voce precisa guardar):
 *
 *   ENVIRONMENT  = cluster/ambiente  -> brokers, certificados, senhas, registry
 *   PASTA        = aplicacao          -> qual certificado/usuario usar (kafkaApp)
 *   REQUEST      = evento/topico      -> kafkaTopic, key, headers, body
 *
 * Ordem de resolucao de QUALQUER variavel `x`, da maior para a menor prioridade:
 *
 *   1. var de request/pasta   `x_<kafkaApp>`
 *   2. var de request/pasta   `x`
 *   3. var de environment     `x_<kafkaApp>`
 *   4. var de environment     `x`
 *   5. variavel de ambiente do SO  `X_<KAFKA_APP>` / `X`  (so p/ CI, ver README)
 *
 * O sufixo `_<kafkaApp>` e o que permite VARIOS keystores/usuarios no MESMO
 * cluster sem clonar environment: no environment `hml` voce tem
 *   kafkaKeystore: default.jks
 *   kafkaKeystore_pagamentos: pagamentos.jks
 * e a pasta `pagamentos/` so precisa declarar `kafkaApp: pagamentos`.
 */

const fs = require('fs');
const path = require('path');

function isBlank(value) {
  return value === undefined || value === null || String(value).trim() === '';
}

function firstFilled(...values) {
  for (const value of values) if (!isBlank(value)) return String(value).trim();
  return undefined;
}

/** kafkaTruststorePassword -> KAFKA_TRUSTSTORE_PASSWORD */
function toEnvKey(name) {
  return name.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toUpperCase();
}

/**
 * Cria o leitor de variaveis com a cadeia de prioridade descrita no topo.
 * `aliases` permite aceitar nomes antigos sem quebrar quem ja configurou
 * (ex: kafkaTruststorePath continua funcionando como kafkaTruststore).
 */
function createReader(bru) {
  const app = firstFilled(bru.getVar('kafkaApp'), bru.getEnvVar('kafkaApp'));

  const readOne = (name) => {
    const names = app ? [`${name}_${app}`, name] : [name];
    for (const n of names) {
      const value = bru.getVar(n);
      if (!isBlank(value)) return String(value).trim();
    }
    for (const n of names) {
      const value = bru.getEnvVar(n);
      if (!isBlank(value)) return String(value).trim();
    }
    for (const n of names) {
      const value = process.env[toEnvKey(n)];
      if (!isBlank(value)) return String(value).trim();
    }
    return undefined;
  };

  const read = (name, ...aliases) => firstFilled(...[name, ...aliases].map(readOne));

  read.app = app;
  read.bool = (name, fallback = false) => {
    const value = read(name);
    if (value === undefined) return fallback;
    return /^(true|1|yes|sim|on)$/i.test(value);
  };
  read.int = (name, fallback) => {
    const value = read(name);
    if (value === undefined) return fallback;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  };
  read.list = (name) => {
    const value = read(name);
    return value === undefined ? [] : value.split(',').map((i) => i.trim()).filter(Boolean);
  };

  return read;
}

/**
 * Resolve o caminho de um certificado. Aceita:
 *   - caminho absoluto        -> C:/certs/pagamentos.jks
 *   - nome relativo + kafkaCertsDir -> "pagamentos.jks" + "C:/certs"
 *   - nome relativo sem certsDir    -> relativo a raiz da collection
 *
 * Assim o environment versionado no Git guarda so o NOME do arquivo (igual pra
 * todo mundo) e cada maquina ajusta uma unica variavel: kafkaCertsDir.
 */
function resolveCertPath(read, collectionRoot, value) {
  if (isBlank(value)) return undefined;
  const raw = String(value).trim().replace(/\\/g, '/');
  if (path.isAbsolute(raw) || /^[a-zA-Z]:\//.test(raw)) return raw;
  const certsDir = read('kafkaCertsDir');
  return path.join(certsDir ? certsDir.replace(/\\/g, '/') : collectionRoot, raw);
}

/**
 * Config de conexao com o cluster: brokers + SSL + SASL.
 * Compartilhada por producer, consumer e doctor.
 */
function buildConnectionConfig(read, collectionRoot) {
  return {
    brokers: read.list('kafkaBrokers'),
    clientId: read('kafkaClientId') || 'bruno-kafka',
    ssl: {
      enabled: read.bool('kafkaSslEnabled', false),
      rejectUnauthorized: read.bool('kafkaSslRejectUnauthorized', true),
      truststore: {
        path: resolveCertPath(read, collectionRoot, read('kafkaTruststore', 'kafkaTruststorePath')),
        password: read('kafkaTruststorePassword'),
      },
      keystore: {
        path: resolveCertPath(read, collectionRoot, read('kafkaKeystore', 'kafkaKeystorePath')),
        password: read('kafkaKeystorePassword'),
      },
    },
    sasl: {
      enabled: read.bool('kafkaSaslEnabled', false),
      mechanism: read('kafkaSaslMechanism') || 'plain',
      username: read('kafkaSaslUsername'),
      password: read('kafkaSaslPassword'),
    },
  };
}

/**
 * Config do Schema Registry (opcional). O schema a usar sai, nessa ordem:
 *   kafkaSchemaId > kafkaSchemaInline > kafkaSchemaFile > ultimo do subject
 * O caso mais comum (e o default) e o ultimo: nada configurado, usa o schema
 * mais recente publicado em `<topico>-value`.
 */
function buildSchemaRegistryConfig(read, collectionRoot, topic) {
  if (!read.bool('kafkaSchemaRegistryEnabled', false)) return undefined;

  const id = read('kafkaSchemaId', 'kafkaValueSchemaId');
  const inline = read('kafkaSchemaInline', 'kafkaValueSchemaInline');
  const file = read('kafkaSchemaFile', 'kafkaValueSchemaFile');

  let schema;
  if (isBlank(id)) {
    if (!isBlank(inline)) {
      schema = inline;
    } else if (!isBlank(file)) {
      const schemaPath = path.isAbsolute(file) ? file : path.join(collectionRoot, file);
      if (!fs.existsSync(schemaPath)) {
        throw new Error(
          `Schema Registry: arquivo de schema nao encontrado em "${schemaPath}" ` +
          '(variavel kafkaSchemaFile). Corrija o caminho ou limpe a variavel para ' +
          'usar o ultimo schema publicado no subject.'
        );
      }
      schema = fs.readFileSync(schemaPath, 'utf-8');
    }
  }

  const username = read('kafkaSchemaRegistryUsername');

  return {
    enabled: true,
    url: read('kafkaSchemaRegistryUrl'),
    auth: username ? { username, password: read('kafkaSchemaRegistryPassword') } : undefined,
    value: {
      id: isBlank(id) ? undefined : Number(id),
      type: (read('kafkaSchemaType', 'kafkaValueSchemaType') || 'AVRO').toUpperCase(),
      subject: read('kafkaSchemaSubject', 'kafkaValueSchemaSubject') || `${topic}-value`,
      schema,
    },
  };
}

/**
 * Junta tudo e valida o que e obrigatorio, com mensagens que dizem QUAL
 * variavel preencher e ONDE - em vez de estourar um TypeError generico.
 *
 * @param {object} bru               objeto `bru` do Bruno
 * @param {object} [opts]
 * @param {boolean} [opts.requireTopic=true]
 */
function resolveConfig(bru, opts = {}) {
  const { requireTopic = true } = opts;
  const collectionRoot = bru.cwd();
  const read = createReader(bru);

  const topic = read('kafkaTopic');
  const problems = [];

  if (!read('kafkaBrokers')) {
    problems.push(
      'kafkaBrokers esta vazio. Selecione um environment (canto superior direito) ' +
      'e preencha os brokers, ex: "broker1:9092,broker2:9092".'
    );
  }
  if (requireTopic && !topic) {
    problems.push(
      'kafkaTopic esta vazio. Ele e uma variavel DO REQUEST (aba Vars > Pre Request) ' +
      'ou da pasta - nao do environment.'
    );
  }

  const connection = buildConnectionConfig(read, collectionRoot);

  for (const kind of ['truststore', 'keystore']) {
    const store = connection.ssl[kind];
    if (!store.path) continue;
    if (!fs.existsSync(store.path)) {
      problems.push(
        `${kind}: arquivo nao encontrado em "${store.path}". Confira kafkaCertsDir ` +
        `e kafka${kind === 'truststore' ? 'Truststore' : 'Keystore'} no environment.`
      );
    } else if (!/\.(pem|crt|cer|key)$/i.test(store.path) && isBlank(store.password)) {
      problems.push(
        `${kind}: falta a senha. Preencha a variavel SECRETA ` +
        `kafka${kind === 'truststore' ? 'Truststore' : 'Keystore'}Password` +
        `${read.app ? `_${read.app}` : ''} na tela de Environment.`
      );
    }
  }

  if (connection.sasl.enabled) {
    if (!connection.sasl.username) problems.push('SASL ligado mas kafkaSaslUsername esta vazio.');
    if (!connection.sasl.password) {
      problems.push('SASL ligado mas a variavel secreta kafkaSaslPassword esta vazia.');
    }
  }

  const schemaRegistry = buildSchemaRegistryConfig(read, collectionRoot, topic);
  if (schemaRegistry && !schemaRegistry.url) {
    problems.push('Schema Registry ligado mas kafkaSchemaRegistryUrl esta vazio.');
  }

  if (problems.length) {
    throw new Error(`Configuracao incompleta:\n  - ${problems.join('\n  - ')}`);
  }

  return {
    read,
    app: read.app,
    collectionRoot,
    topic,
    environmentName: typeof bru.getEnvName === 'function' ? bru.getEnvName() : undefined,
    isProduction: read.bool('kafkaIsProduction', false),
    allowProduction: read.bool('kafkaAllowProduction', false),
    dryRun: read.bool('kafkaDryRun', false),
    ...connection,
    schemaRegistry,
  };
}

module.exports = {
  resolveConfig,
  createReader,
  buildConnectionConfig,
  buildSchemaRegistryConfig,
  resolveCertPath,
  isBlank,
  toEnvKey,
};
