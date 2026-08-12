/**
 * bruno-kafka.js
 * --------------
 * A UNICA coisa que um request precisa chamar. Toda a logica (config, headers,
 * key, schema, dry-run, trava de producao, relatorio) mora aqui, entao o
 * `script:pre-request` de cada request tem UMA linha e nunca precisa ser
 * atualizado quando a logica mudar.
 *
 *   const path = require('path');
 *   await require(path.join(bru.cwd(), 'lib', 'bruno-kafka.js')).produce({ req, bru });
 *
 * O request em si vira 100% declarativo:
 *   - aba Vars (Pre Request) -> kafkaTopic, kafkaKey, ...
 *   - aba Headers            -> headers da MENSAGEM Kafka
 *   - aba Body               -> payload (aceita {{variaveis}} do Bruno)
 */

const fs = require('fs');
const path = require('path');

const { resolveConfig, createReader, buildConnectionConfig, buildSchemaRegistryConfig } = require('./kafka-config');
const { sendKafkaMessage } = require('./kafka-producer');
const { consumeLastMessages } = require('./kafka-consumer');
const { runDiagnostics, hintFor, STATUS } = require('./kafka-doctor');

const LINE = '-'.repeat(64);

function box(title, lines) {
  return [`\n${LINE}\n Kafka - ${title}`, ...lines.map((l) => ` ${l}`), LINE].join('\n');
}

function field(label, value) {
  return `${String(label).padEnd(12)}${value}`;
}

/**
 * Fora do Collection Runner o Bruno SEMPRE dispara a requisicao HTTP do request.
 * Dentro do Runner (o fluxo recomendado desta collection) `skipRequest()` cancela
 * essa chamada, entao nada sai pela rede alem do proprio Kafka.
 */
function skipHttp(bru) {
  try {
    if (bru.runner && typeof bru.runner.skipRequest === 'function') bru.runner.skipRequest();
  } catch (err) { /* fora do runner nao existe: a URL padrao aponta pra 127.0.0.1 */ }
}

/**
 * Le o Body do request como valor da mensagem.
 * - JSON  -> objeto (interpolando {{variaveis}} do Bruno antes)
 * - texto -> string crua
 * - vazio -> null, que o producer publica como TOMBSTONE (topico compactado)
 */
function readMessageValue(req, bru) {
  const body = req.getBody();
  if (body === undefined || body === null) return null;

  const interpolate = (text) => {
    if (typeof bru.interpolate !== 'function') return text;
    try { return bru.interpolate(text); } catch (err) { return text; }
  };

  if (typeof body === 'string') {
    const text = interpolate(body).trim();
    if (!text) return null;
    try { return JSON.parse(text); } catch (err) { return text; }
  }

  try { return JSON.parse(interpolate(JSON.stringify(body))); } catch (err) { return body; }
}

/** Aba Headers do request = headers da mensagem Kafka (menos os de HTTP). */
function readMessageHeaders(req) {
  const ignored = new Set(['content-type', 'accept', 'user-agent', 'content-length']);
  const headers = {};
  for (const [name, value] of Object.entries(req.getHeaders() || {})) {
    if (ignored.has(String(name).toLowerCase())) continue;
    if (value === undefined || value === null || String(value) === '') continue;
    headers[name] = String(value);
  }
  return Object.keys(headers).length ? headers : undefined;
}

/**
 * Trava de producao: com o environment marcado `kafkaIsProduction: true` (ou
 * chamado "prod"), o envio so acontece com `kafkaAllowProduction: true`.
 * Evita o classico "estava com prod selecionado e nem percebi".
 */
function guardProduction(config) {
  const looksLikeProd = config.isProduction || /pro?d/i.test(config.environmentName || '');
  if (!looksLikeProd || config.allowProduction || config.dryRun) return;
  throw new Error(
    `PRODUCAO BLOQUEADA: o environment "${config.environmentName || '?'}" e de producao.\n` +
    'Se voce REALMENTE quer publicar no topico de producao, abra a tela de Environment ' +
    'e mude kafkaAllowProduction para true (e volte para false depois).\n' +
    'Para so validar o payload sem publicar, use kafkaDryRun: true.'
  );
}

function fail(bru, statusVar, err) {
  const hint = hintFor(err);
  bru.setVar(statusVar, 'error');
  bru.setVar('kafkaError', err.message);
  throw new Error(`${err.message}${hint ? `\n>> Causa provavel: ${hint}` : ''}`);
}

/* ------------------------------------------------------------------------- */
/* PRODUZIR                                                                   */
/* ------------------------------------------------------------------------- */

async function produce({ req, bru }) {
  bru.setVar('kafkaSendStatus', 'error');

  let config;
  try {
    config = resolveConfig(bru);
    guardProduction(config);
  } catch (err) {
    bru.setVar('kafkaError', err.message);
    throw err;
  }

  const value = readMessageValue(req, bru);
  const headers = readMessageHeaders(req);
  const key = config.read('kafkaKey');
  const startedAt = Date.now();

  try {
    const result = await sendKafkaMessage({
      brokers: config.brokers,
      clientId: config.clientId,
      topic: config.topic,
      key,
      value,
      headers,
      partition: config.read.int('kafkaPartition', undefined),
      acks: config.read.int('kafkaAcks', undefined),
      timeout: config.read.int('kafkaTimeoutMs', undefined),
      ssl: config.ssl,
      sasl: config.sasl,
      schemaRegistry: config.schemaRegistry,
      dryRun: config.dryRun,
    });

    const elapsed = Date.now() - startedAt;
    const record = result.records[0];
    const lines = [
      field('ambiente', `${config.environmentName || '?'}${config.app ? `   (aplicacao: ${config.app})` : ''}`),
      field('topico', `${config.topic}${key ? `   key: ${key}` : '   (sem key)'}`),
    ];
    if (config.schemaRegistry) {
      lines.push(field('schema', `${config.schemaRegistry.value.type} id=${result.schemaId} (${config.schemaRegistry.value.subject})`));
    }
    if (headers) lines.push(field('headers', Object.keys(headers).join(', ')));
    if (value === null) lines.push(field('payload', 'TOMBSTONE (valor nulo)'));

    if (result.dryRun) {
      lines.push(`[DRY RUN] payload valido, ${result.bytes} bytes - NADA foi publicado (kafkaDryRun=true)`);
      bru.setVar('kafkaSendStatus', 'dryrun');
      bru.setVar('kafkaSendSummary', `dry run ok em ${config.topic}`);
    } else {
      lines.push(`[OK] publicado   particao ${record.partition}   offset ${record.baseOffset}   ${result.bytes} bytes   ${elapsed}ms`);
      bru.setVar('kafkaSendStatus', 'success');
      bru.setVar('kafkaSendPartition', String(record.partition));
      bru.setVar('kafkaSendOffset', String(record.baseOffset));
      bru.setVar('kafkaSendSummary', `${config.topic} p${record.partition}@${record.baseOffset}`);
    }

    console.log(box(result.dryRun ? 'dry run' : 'produzir', lines));
    skipHttp(bru);
    return result;
  } catch (err) {
    console.error(box('produzir - FALHOU', [
      field('ambiente', config.environmentName || '?'),
      field('topico', config.topic),
      field('erro', err.message),
    ]));
    return fail(bru, 'kafkaSendStatus', err);
  }
}

/* ------------------------------------------------------------------------- */
/* CONSUMIR (conferir o que chegou)                                           */
/* ------------------------------------------------------------------------- */

async function consume({ bru }) {
  bru.setVar('kafkaConsumeStatus', 'error');
  const config = resolveConfig(bru);

  const max = config.read.int('kafkaMaxMessages', 10);
  const timeoutMs = config.read.int('kafkaTimeoutMs', 10000);
  const fromBeginning = config.read.bool('kafkaFromBeginning', false);

  try {
    const result = await consumeLastMessages({
      brokers: config.brokers,
      clientId: config.clientId,
      topic: config.topic,
      ssl: config.ssl,
      sasl: config.sasl,
      schemaRegistry: config.schemaRegistry,
      max,
      timeoutMs,
      fromBeginning,
    });

    const outPath = path.join(config.collectionRoot, 'out', 'last-consume.json');
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(result, null, 2), 'utf-8');

    const lines = [
      field('ambiente', config.environmentName || '?'),
      field('topico', `${config.topic}   ${result.partitions.length} particao(oes)`),
      field('lidas', `${result.messages.length} de ate ${max}${result.empty ? ' (topico vazio)' : ''}`),
      field('arquivo', outPath),
      '',
    ];
    for (const message of result.messages) {
      const payload = typeof message.value === 'string' ? message.value : JSON.stringify(message.value);
      lines.push(`p${message.partition}@${message.offset}  ${message.timestamp}  key=${message.key === null ? '-' : message.key}`);
      lines.push(`   ${payload === null ? 'TOMBSTONE' : payload.slice(0, 300)}`);
      if (Object.keys(message.headers).length) lines.push(`   headers: ${JSON.stringify(message.headers)}`);
    }

    console.log(box('consumir', lines));
    bru.setVar('kafkaConsumeStatus', 'success');
    bru.setVar('kafkaMessagesCount', String(result.messages.length));
    bru.setVar('kafkaMessages', JSON.stringify(result.messages));
    bru.setVar('kafkaLastMessage', JSON.stringify(result.messages[0] || null));
    skipHttp(bru);
    return result;
  } catch (err) {
    console.error(box('consumir - FALHOU', [field('topico', config.topic), field('erro', err.message)]));
    return fail(bru, 'kafkaConsumeStatus', err);
  }
}

/* ------------------------------------------------------------------------- */
/* DOCTOR                                                                     */
/* ------------------------------------------------------------------------- */

const ICON = { [STATUS.OK]: '[ok]  ', [STATUS.FAIL]: '[FALHA]', [STATUS.WARN]: '[aviso]', [STATUS.SKIP]: '[--]  ' };

async function doctor({ bru }) {
  // O doctor NAO usa resolveConfig: ele precisa RELATAR config faltando em vez
  // de estourar no primeiro problema.
  const read = createReader(bru);
  const collectionRoot = bru.cwd();
  const topic = read('kafkaTopic');

  let schemaRegistry;
  try {
    schemaRegistry = buildSchemaRegistryConfig(read, collectionRoot, topic);
  } catch (err) {
    schemaRegistry = undefined;
  }

  const { checks, ok } = await runDiagnostics({
    topic,
    collectionRoot,
    app: read.app,
    environmentName: typeof bru.getEnvName === 'function' ? bru.getEnvName() : undefined,
    isProduction: read.bool('kafkaIsProduction', false) || /pro?d/i.test((typeof bru.getEnvName === 'function' && bru.getEnvName()) || ''),
    connection: buildConnectionConfig(read, collectionRoot),
    schemaRegistry,
  });

  const lines = [];
  for (const check of checks) {
    lines.push(`${ICON[check.status]} ${check.name}`);
    if (check.detail) lines.push(`        ${check.detail}`);
    if (check.hint) lines.push(`        -> ${check.hint}`);
  }
  lines.push('');
  lines.push(ok ? 'Tudo pronto para enviar.' : 'Corrija os itens marcados como [FALHA] acima.');

  console.log(box('doctor', lines));
  bru.setVar('kafkaDoctorOk', String(ok));
  bru.setVar('kafkaDoctorReport', JSON.stringify(checks));
  skipHttp(bru);

  if (!ok) {
    const first = checks.find((c) => c.status === STATUS.FAIL);
    throw new Error(`${first.name}: ${first.detail}${first.hint ? `\n>> ${first.hint}` : ''}`);
  }
  return checks;
}

module.exports = { produce, consume, doctor };
