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
const { serveOnce, assertLoopbackUrl } = require('./local-echo');

const LINE = '-'.repeat(64);

function box(title, lines) {
  return [`\n${LINE}\n Kafka - ${title}`, ...lines.map((l) => ` ${l}`), LINE].join('\n');
}

function field(label, value) {
  return `${String(label).padEnd(12)}${value}`;
}

/**
 * O Bruno SEMPRE dispara a requisicao HTTP do request quando voce clica em Send.
 * Em vez de deixar essa chamada sair, apontamos ela para um eco local efemero
 * (ver lib/local-echo.js) que devolve o resultado do Kafka: a aba Response mostra
 * 200 verde com particao/offset e nenhum byte sai da maquina.
 *
 * No Collection Runner o `skipRequest()` cancela a chamada antes disso; o eco
 * simplesmente expira sozinho em alguns segundos.
 */
function skipRunnerHttp(bru) {
  try {
    if (bru.runner && typeof bru.runner.skipRequest === 'function') bru.runner.skipRequest();
  } catch (err) { /* fora do runner nao existe */ }
}

/** URL de destino da chamada HTTP do request, com as {{variaveis}} resolvidas. */
function readRequestUrl(req, bru) {
  if (!req || typeof req.getUrl !== 'function') return undefined;
  let url;
  try { url = req.getUrl(); } catch (err) { return undefined; }
  if (typeof bru.interpolate === 'function') {
    try { url = bru.interpolate(url); } catch (err) { /* usa a URL crua */ }
  }
  return url;
}

/**
 * Entrega o resultado na aba Response via eco local. Se a versao do Bruno nao
 * tiver `req.setUrl`, nao faz nada: a URL que fica no .bru aponta para uma porta
 * morta em 127.0.0.1, entao o pior caso continua sendo "nada sai da maquina".
 */
async function deliver({ req, bru, report }) {
  skipRunnerHttp(bru);
  if (!req || typeof req.setUrl !== 'function') return undefined;

  try {
    const echo = await serveOnce(report);
    req.setUrl(echo.url);
    // O payload nao precisa nem trafegar pelo loopback: o eco ignora o corpo.
    try { if (typeof req.setMethod === 'function') req.setMethod('GET'); } catch (err) { /* opcional */ }
    try { if (typeof req.setBody === 'function') req.setBody(null); } catch (err) { /* opcional */ }
    return echo;
  } catch (err) {
    console.warn(`Kafka: nao foi possivel subir o eco local (${err.message}). O envio ao Kafka nao foi afetado.`);
    return undefined;
  }
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
    // Antes de montar o payload: se a URL do request apontar para fora da
    // maquina, nada e publicado e nada e enviado.
    assertLoopbackUrl(readRequestUrl(req, bru));
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

    await deliver({
      req,
      bru,
      report: {
        status: result.dryRun ? 'dry-run (nada foi publicado)' : 'publicado',
        ambiente: config.environmentName || null,
        aplicacao: config.app || null,
        topico: config.topic,
        key: key === undefined ? null : key,
        particao: result.dryRun ? null : record.partition,
        offset: result.dryRun ? null : record.baseOffset,
        bytes: result.bytes,
        tempo: `${elapsed}ms`,
        tombstone: value === null ? true : undefined,
        schema: config.schemaRegistry ? {
          tipo: config.schemaRegistry.value.type,
          id: result.schemaId,
          subject: config.schemaRegistry.value.subject,
        } : null,
        headers: headers || null,
        payload: value,
      },
    });

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

async function consume({ req, bru }) {
  bru.setVar('kafkaConsumeStatus', 'error');
  assertLoopbackUrl(readRequestUrl(req, bru));
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

    await deliver({
      req,
      bru,
      report: {
        status: result.empty ? 'topico vazio' : 'lido',
        ambiente: config.environmentName || null,
        topico: config.topic,
        lidas: result.messages.length,
        particoes: result.partitions,
        arquivo: outPath,
        mensagens: result.messages,
      },
    });

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

async function doctor({ req, bru }) {
  // O doctor NAO usa resolveConfig: ele precisa RELATAR config faltando em vez
  // de estourar no primeiro problema.
  assertLoopbackUrl(readRequestUrl(req, bru));
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

  await deliver({
    req,
    bru,
    report: {
      status: ok ? 'tudo pronto para enviar' : 'com falhas',
      ambiente: (typeof bru.getEnvName === 'function' ? bru.getEnvName() : null) || null,
      aplicacao: read.app || null,
      topico: topic || null,
      verificacoes: checks,
    },
  });

  if (!ok) {
    const first = checks.find((c) => c.status === STATUS.FAIL);
    throw new Error(`${first.name}: ${first.detail}${first.hint ? `\n>> ${first.hint}` : ''}`);
  }
  return checks;
}

module.exports = { produce, consume, doctor };
