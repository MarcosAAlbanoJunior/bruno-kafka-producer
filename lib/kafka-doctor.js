/**
 * kafka-doctor.js
 * ---------------
 * Diagnostico em um clique. Roda as verificacoes NA ORDEM em que elas costumam
 * quebrar num ambiente corporativo e para na primeira que impede as seguintes,
 * devolvendo um checklist legivel em vez de um stack trace.
 *
 * Objetivo: transformar "nao funciona" em "o truststore abriu, o broker
 * respondeu, mas o topico nao existe nesse cluster" sem ninguem precisar
 * abrir DevTools.
 */

const fs = require('fs');
const path = require('path');
const https = require('https');
const { Kafka, logLevel } = require('kafkajs');
const { buildSslConfig, loadStore } = require('./kafka-producer');
const { requestJson } = require('./http-json');

const OK = 'ok';
const FAIL = 'fail';
const WARN = 'warn';
const SKIP = 'skip';

/** Traduz erros comuns de rede/TLS/auth para a causa provavel. */
function hintFor(err) {
  const message = String((err && err.message) || err);
  if (/ECONNREFUSED/i.test(message)) return 'Nada escutando nesse host:porta. Broker errado ou servico fora do ar.';
  if (/ETIMEDOUT|ENETUNREACH|EHOSTUNREACH|timeout/i.test(message)) return 'Sem rota ate o cluster. Quase sempre e VPN desligada ou firewall.';
  if (/ENOTFOUND|EAI_AGAIN/i.test(message)) return 'DNS nao resolveu o hostname. Confira o nome do broker (e a VPN).';
  if (/self.signed|unable to verify|UNABLE_TO_GET_ISSUER|CERT_/i.test(message)) return 'A CA que assinou o servidor nao esta no truststore. Confira kafkaTruststore.';
  if (/handshake|SSL|TLS/i.test(message)) return 'Falha de TLS. Confira kafkaSslEnabled, o truststore e (se mTLS) o keystore.';
  if (/SASL|authentic|Authentication/i.test(message)) return 'Usuario/senha SASL recusados. Confira kafkaSaslUsername e a variavel secreta kafkaSaslPassword.';
  if (/not authorized|TOPIC_AUTHORIZATION|GROUP_AUTHORIZATION/i.test(message)) return 'Conectou, mas o usuario nao tem ACL para esse topico/grupo.';
  if (/401|Unauthorized/i.test(message)) return 'Schema Registry recusou a autenticacao. Confira usuario e a variavel secreta kafkaSchemaRegistryPassword.';
  return undefined;
}

const DEPENDENCIES = ['kafkajs', 'jks-js', '@kafkajs/confluent-schema-registry'];

/** Lista as dependencias com a versao instalada, quando der para descobrir. */
function describeDependencies(collectionRoot) {
  return DEPENDENCIES.map((dep) => {
    if (!collectionRoot) return dep;
    try {
      const pkg = path.join(collectionRoot, 'node_modules', ...dep.split('/'), 'package.json');
      return `${dep} ${JSON.parse(fs.readFileSync(pkg, 'utf-8')).version}`;
    } catch (err) {
      return `${dep} (carregado)`;
    }
  }).join(', ');
}

/**
 * @param {object} ctx  { topic?, environmentName?, app?, isProduction?, collectionRoot?, connection, schemaRegistry? }
 * @returns {Promise<{checks:object[], ok:boolean}>}
 */
async function runDiagnostics(ctx) {
  const { topic, environmentName, app, isProduction, collectionRoot, connection, schemaRegistry } = ctx;
  const checks = [];
  const add = (name, status, detail, hint) => { checks.push({ name, status, detail, hint }); return status; };

  // 1. Dependencias npm ------------------------------------------------------
  // Nao usamos require.resolve aqui: o `require` do sandbox do Bruno e um shim
  // e nao tem .resolve. Na pratica, se as dependencias faltassem este arquivo
  // nem teria carregado ("Cannot find module 'kafkajs'"), entao o que interessa
  // e confirmar que carregaram e MOSTRAR a versao - util no suporte.
  if (typeof Kafka !== 'function') {
    add('Dependencias npm', FAIL, 'kafkajs nao carregou corretamente',
      'Rode "npm install" na pasta da collection e reabra o Bruno.');
    return { checks, ok: false };
  }
  add('Dependencias npm', OK, describeDependencies(collectionRoot));

  // 2. Configuracao basica ---------------------------------------------------
  const brokers = connection.brokers || [];
  if (!brokers.length) {
    add('Variaveis de conexao', FAIL, 'kafkaBrokers vazio',
      'Selecione um environment no canto superior direito e preencha kafkaBrokers.');
    return { checks, ok: false };
  }
  add('Variaveis de conexao', OK,
    `environment "${environmentName || '?'}"${app ? `, aplicacao "${app}"` : ''}, brokers: ${brokers.join(', ')}`);

  // 3. Certificados ----------------------------------------------------------
  if (!connection.ssl.enabled) {
    add('Certificados (TLS)', SKIP, 'kafkaSslEnabled=false - conexao em texto puro');
  } else {
    for (const kind of ['truststore', 'keystore']) {
      const store = connection.ssl[kind];
      const label = `Certificado: ${kind}`;
      if (!store.path) {
        add(label, SKIP, `kafka${kind === 'truststore' ? 'Truststore' : 'Keystore'} nao informado`);
        continue;
      }
      if (!fs.existsSync(store.path)) {
        add(label, FAIL, `arquivo nao encontrado: ${store.path}`,
          'Confira kafkaCertsDir (o diretorio) e o nome do arquivo no environment.');
        continue;
      }
      try {
        const loaded = loadStore(store.path, store.password, kind);
        const parts = [];
        if (loaded.ca && loaded.ca.length) parts.push(`${loaded.ca.length} CA(s)`);
        if (loaded.cert) parts.push('certificado de client');
        if (loaded.key) parts.push('chave privada');
        add(label, OK, `${store.path} -> ${parts.join(' + ') || 'nenhuma entrada util'}`);
      } catch (err) {
        add(label, FAIL, err.message,
          /senha/i.test(err.message)
            ? `Preencha a variavel secreta kafka${kind === 'truststore' ? 'Truststore' : 'Keystore'}Password na tela de Environment.`
            : 'Senha incorreta ou arquivo corrompido/em formato inesperado.');
      }
    }
  }

  const sslConfig = (() => {
    try { return buildSslConfig(connection.ssl); } catch (err) { return undefined; }
  })();

  // 4. Conexao com o cluster -------------------------------------------------
  const kafka = new Kafka({
    clientId: connection.clientId || 'bruno-kafka-doctor',
    brokers,
    ssl: sslConfig,
    sasl: connection.sasl.enabled ? {
      mechanism: connection.sasl.mechanism || 'plain',
      username: connection.sasl.username,
      password: connection.sasl.password,
    } : undefined,
    logLevel: logLevel.NOTHING,
    connectionTimeout: 10000,
    retry: { retries: 0 },
  });

  const admin = kafka.admin();
  let topics;
  const startedAt = Date.now();
  try {
    await admin.connect();
    topics = await admin.listTopics();
    add('Conexao com o cluster', OK,
      `${topics.length} topico(s) visiveis em ${Date.now() - startedAt}ms` +
      `${connection.sasl.enabled ? ` (SASL ${connection.sasl.mechanism} como "${connection.sasl.username}")` : ''}`);
  } catch (err) {
    add('Conexao com o cluster', FAIL, err.message, hintFor(err));
    await admin.disconnect().catch(() => {});
  }

  // 5. Topico ----------------------------------------------------------------
  // O Schema Registry (item 6) e independente do broker, entao seguimos mesmo
  // se a conexao acima falhou - o objetivo e dar o retrato COMPLETO de uma vez.
  try {
    if (!topics) {
      add('Topico', SKIP, 'sem conexao com o cluster para verificar');
    } else if (!topic) {
      add('Topico', SKIP, 'nenhum kafkaTopic definido neste request');
    } else if (!topics.includes(topic)) {
      const parecidos = topics.filter((t) => t.toLowerCase().includes(topic.toLowerCase().slice(0, 6))).slice(0, 5);
      add('Topico', FAIL, `"${topic}" nao existe neste cluster`,
        parecidos.length ? `Parecidos por aqui: ${parecidos.join(', ')}` : 'Confira o nome em kafkaTopic (e se e o cluster certo).');
    } else {
      const offsets = await admin.fetchTopicOffsets(topic);
      const total = offsets.reduce((sum, p) => sum + (Number(p.high) - Number(p.low)), 0);
      add('Topico', OK, `"${topic}": ${offsets.length} particao(oes), ~${total} mensagem(ns) retidas`);
    }
  } catch (err) {
    add('Topico', FAIL, err.message, hintFor(err));
  } finally {
    await admin.disconnect().catch(() => {});
  }

  // 6. Schema Registry -------------------------------------------------------
  if (!schemaRegistry || !schemaRegistry.enabled) {
    add('Schema Registry', SKIP, 'kafkaSchemaRegistryEnabled=false');
  } else {
    const base = String(schemaRegistry.url).replace(/\/+$/, '');
    const agent = /^https:/i.test(base) && sslConfig
      ? new https.Agent({
        ca: sslConfig.ca,
        cert: sslConfig.cert,
        key: sslConfig.key,
        rejectUnauthorized: sslConfig.rejectUnauthorized !== false,
      })
      : undefined;
    const http = { auth: schemaRegistry.auth, agent, timeoutMs: 8000 };
    try {
      const subjects = await requestJson(`${base}/subjects`, http);
      add('Schema Registry', OK, `${base} respondeu com ${subjects.length} subject(s)`);

      const subject = (schemaRegistry.value && schemaRegistry.value.subject) || (topic ? `${topic}-value` : undefined);
      if (!subject) {
        add('Subject do topico', SKIP, 'sem topico definido');
      } else if (subjects.includes(subject)) {
        const latest = await requestJson(`${base}/subjects/${encodeURIComponent(subject)}/versions/latest`, http);
        add('Subject do topico', OK, `"${subject}" na versao ${latest.version} (schema id ${latest.id})`);
      } else {
        add('Subject do topico', WARN, `"${subject}" ainda nao existe no registry`,
          'Se voce nao vai registrar o schema pelo Bruno (kafkaSchemaInline/kafkaSchemaFile), o envio vai falhar com "Subject not found".');
      }
    } catch (err) {
      add('Schema Registry', FAIL, err.message, hintFor(err));
    }
  }

  // 7. Aviso de producao -----------------------------------------------------
  if (isProduction) {
    add('Ambiente', WARN, `"${environmentName || '?'}" esta marcado como PRODUCAO`,
      'Envios exigem kafkaAllowProduction=true. Deixe assim so enquanto precisar.');
  }

  return { checks, ok: !checks.some((c) => c.status === FAIL) };
}

module.exports = { runDiagnostics, hintFor, STATUS: { OK, FAIL, WARN, SKIP } };
