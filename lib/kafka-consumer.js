/**
 * kafka-consumer.js
 * -----------------
 * Le as ULTIMAS N mensagens de um topico, sem precisar de outra ferramenta.
 *
 * E o outro lado do ciclo do dia a dia: produziu -> quer conferir se chegou,
 * com que key, quais headers e (se houver Schema Registry) o payload Avro ja
 * decodificado de volta para JSON legivel.
 *
 * Nao "consome de verdade" no sentido de aplicacao: usa um consumer group
 * descartavel (`bruno-viewer-...`), posiciona o offset em (fim - N) e sai,
 * apagando o group no final. Nao interfere no offset dos consumers reais.
 */

const { Kafka, logLevel } = require('kafkajs');
const { buildSslConfig, createRegistry } = require('./kafka-producer');

/**
 * Tenta devolver o payload no formato mais legivel possivel:
 * Avro/Protobuf/JSON-Schema (via registry) > JSON > texto > base64.
 */
async function decodePayload(buffer, registry) {
  if (buffer === null || buffer === undefined) return null;

  // Formato wire do Confluent: primeiro byte 0 + 4 bytes de schema id.
  if (registry && buffer.length > 5 && buffer[0] === 0) {
    try {
      return await registry.decode(buffer);
    } catch (err) {
      return { _erroAoDecodificar: err.message, _base64: buffer.toString('base64') };
    }
  }

  const text = buffer.toString('utf-8');
  try {
    return JSON.parse(text);
  } catch (err) {
    // eslint-disable-next-line no-control-regex
    return /[\x00-\x08\x0e-\x1f]/.test(text) ? { _binario: buffer.toString('base64') } : text;
  }
}

function decodeHeaders(headers) {
  const out = {};
  for (const [name, value] of Object.entries(headers || {})) {
    out[name] = Buffer.isBuffer(value) ? value.toString('utf-8') : String(value);
  }
  return out;
}

/**
 * Decide onde comecar a ler em cada particao para pegar as ULTIMAS N mensagens:
 * fim - N, respeitando o inicio da retencao (`low`) quando a particao tem menos
 * que N mensagens. Com `fromBeginning`, comeca do inicio da retencao.
 *
 * @param {Array<{partition:number, low:string|number, high:string|number}>} offsets
 * @returns {Array<{partition:number, low:number, high:number, start:number, pending:number}>}
 */
function planPartitions(offsets, max, fromBeginning = false) {
  return offsets.map((p) => {
    const low = Number(p.low);
    const high = Number(p.high);
    const start = fromBeginning ? low : Math.max(low, high - max);
    return { partition: p.partition, low, high, start, pending: high - start };
  });
}

/**
 * @param {object} opts
 * @param {string[]} opts.brokers
 * @param {string} opts.topic
 * @param {number} [opts.max=10]          quantas mensagens (por particao) buscar
 * @param {number} [opts.timeoutMs=10000] tempo maximo de espera
 * @param {boolean} [opts.fromBeginning]  le desde o inicio em vez das ultimas N
 * @param {object} [opts.ssl] @param {object} [opts.sasl] @param {object} [opts.schemaRegistry]
 * @returns {Promise<{topic:string, partitions:object[], messages:object[]}>}
 */
async function consumeLastMessages(opts) {
  const {
    brokers, clientId, topic, ssl, sasl, schemaRegistry,
    max = 10, timeoutMs = 10000, fromBeginning = false,
  } = opts || {};

  if (!brokers || !brokers.length) throw new Error('Kafka: "brokers" e obrigatorio.');
  if (!topic) throw new Error('Kafka: "topic" e obrigatorio.');

  const sslConfig = buildSslConfig(ssl);
  const kafka = new Kafka({
    clientId: clientId || 'bruno-kafka-viewer',
    brokers,
    ssl: sslConfig,
    sasl: (sasl && sasl.enabled) ? {
      mechanism: sasl.mechanism || 'plain',
      username: sasl.username,
      password: sasl.password,
    } : undefined,
    logLevel: logLevel.NOTHING,
  });

  const registry = (schemaRegistry && schemaRegistry.enabled)
    ? createRegistry(schemaRegistry, sslConfig)
    : undefined;

  const admin = kafka.admin();
  await admin.connect();

  let offsets;
  try {
    offsets = await admin.fetchTopicOffsets(topic);
  } catch (err) {
    await admin.disconnect();
    throw new Error(
      `Nao consegui ler os offsets do topico "${topic}": ${err.message}. ` +
      'O topico existe nesse cluster? (rode o request "Doctor" para conferir)'
    );
  }

  const plan = planPartitions(offsets, max, fromBeginning);

  const expected = plan.reduce((sum, p) => sum + p.pending, 0);
  if (expected === 0) {
    await admin.disconnect();
    return { topic, partitions: plan, messages: [], empty: true };
  }

  const groupId = `bruno-viewer-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
  const collected = [];

  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    await new Promise((resolve, reject) => {
      const timer = setTimeout(resolve, timeoutMs);
      const finish = () => { clearTimeout(timer); resolve(); };

      consumer
        .run({
          eachMessage: async ({ partition, message }) => {
            collected.push({
              partition,
              offset: Number(message.offset),
              timestamp: new Date(Number(message.timestamp)).toISOString(),
              key: message.key ? message.key.toString('utf-8') : null,
              headers: decodeHeaders(message.headers),
              value: await decodePayload(message.value, registry),
            });
            if (collected.length >= expected) finish();
          },
        })
        .then(() => {
          // seek() so vale depois do run(); reposiciona cada particao no ponto
          // calculado acima em vez de ler o topico inteiro.
          for (const p of plan) {
            if (p.pending > 0) consumer.seek({ topic, partition: p.partition, offset: String(p.start) });
          }
        })
        .catch((err) => { clearTimeout(timer); reject(err); });
    });
  } finally {
    await consumer.stop().catch(() => {});
    await consumer.disconnect().catch(() => {});
    // Consumer group descartavel: apaga para nao poluir o cluster.
    await admin.deleteGroups([groupId]).catch(() => {});
    await admin.disconnect().catch(() => {});
  }

  const messages = collected
    .sort((a, b) => (b.timestamp === a.timestamp ? b.offset - a.offset : (b.timestamp > a.timestamp ? 1 : -1)))
    .slice(0, max);

  return { topic, partitions: plan, messages, empty: false };
}

module.exports = { consumeLastMessages, planPartitions, decodePayload, decodeHeaders };
