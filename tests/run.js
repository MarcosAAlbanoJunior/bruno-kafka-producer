const fs = require('fs');
const os = require('os');
const path = require('path');
const { makeBru, makeReq, startMockRegistry, check, expectThrows, assert, summary } = require('./harness');

const ROOT = path.join(__dirname, '..');
const brunoKafka = require(path.join(ROOT, 'lib', 'bruno-kafka.js'));
const { resolveConfig } = require(path.join(ROOT, 'lib', 'kafka-config.js'));
const { sendKafkaMessage } = require(path.join(ROOT, 'lib', 'kafka-producer.js'));
const { consumeLastMessages } = require(path.join(ROOT, 'lib', 'kafka-consumer.js'));
const { runDiagnostics } = require(path.join(ROOT, 'lib', 'kafka-doctor.js'));

const AVRO = JSON.stringify({
  type: 'record', name: 'PedidoValue', namespace: 'exemplos',
  fields: [
    { name: 'orderId', type: 'string' },
    { name: 'status', type: 'string' },
    { name: 'amount', type: 'double' },
  ],
});

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'kb-certs-'));
const pemPath = path.join(tmp, 'truststore.pem');
fs.writeFileSync(pemPath, '-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n');
const jksPath = path.join(tmp, 'app.jks');
fs.writeFileSync(jksPath, Buffer.from('nao-e-um-jks-de-verdade'));

const baseEnv = {
  kafkaBrokers: '127.0.0.1:59092',
  kafkaClientId: 'bruno-kafka',
  kafkaSslEnabled: 'false',
  kafkaSchemaRegistryEnabled: 'false',
};

(async () => {
  console.log('\n== config: prioridade e sufixo por aplicacao ==');

  await check('sufixo _<app> vence a variavel sem sufixo', () => {
    const { bru } = makeBru({
      cwd: ROOT,
      envVars: {
        ...baseEnv,
        kafkaSslEnabled: 'true',
        kafkaCertsDir: tmp,
        kafkaTruststore: 'truststore.pem',
        kafkaKeystore: 'default.pem',
        kafkaKeystore_pagamentos: 'truststore.pem',
        kafkaSaslUsername_pagamentos: 'svc-pagamentos',
      },
      vars: { kafkaApp: 'pagamentos', kafkaTopic: 't' },
    });
    const cfg = resolveConfig(bru);
    assert(cfg.app === 'pagamentos', 'app nao resolvido');
    assert(cfg.ssl.keystore.path.replace(/\\/g, '/').endsWith('truststore.pem'), `keystore errado: ${cfg.ssl.keystore.path}`);
    assert(cfg.brokers.length === 1 && cfg.brokers[0] === '127.0.0.1:59092', 'brokers errados');
  });

  await check('var de request vence var de environment', () => {
    const { bru } = makeBru({
      cwd: ROOT,
      envVars: { ...baseEnv, kafkaTopic: 'do-environment' },
      vars: { kafkaTopic: 'do-request' },
    });
    assert(resolveConfig(bru).topic === 'do-request');
  });

  await check('caminho absoluto de certificado e respeitado', () => {
    const { bru } = makeBru({
      cwd: ROOT,
      envVars: { ...baseEnv, kafkaSslEnabled: 'true', kafkaCertsDir: 'C:/ignorado', kafkaTruststore: pemPath },
      vars: { kafkaTopic: 't' },
    });
    assert(resolveConfig(bru).ssl.truststore.path.replace(/\\/g, '/') === pemPath.replace(/\\/g, '/'));
  });

  console.log('\n== config: mensagens de erro uteis ==');

  await check('sem brokers -> diz o que preencher', () => expectThrows(
    () => resolveConfig(makeBru({ cwd: ROOT, envVars: {}, vars: { kafkaTopic: 't' } }).bru),
    /kafkaBrokers esta vazio.*environment/s
  ));

  await check('sem topico -> explica que topico e do request', () => expectThrows(
    () => resolveConfig(makeBru({ cwd: ROOT, envVars: baseEnv }).bru),
    /kafkaTopic esta vazio.*DO REQUEST/s
  ));

  await check('jks sem senha -> aponta a variavel secreta com sufixo', () => expectThrows(
    () => resolveConfig(makeBru({
      cwd: ROOT,
      envVars: { ...baseEnv, kafkaSslEnabled: 'true', kafkaCertsDir: tmp, kafkaKeystore: 'app.jks' },
      vars: { kafkaTopic: 't', kafkaApp: 'pagamentos' },
    }).bru),
    /kafkaKeystorePassword_pagamentos/
  ));

  await check('certificado inexistente -> mostra o caminho resolvido', () => expectThrows(
    () => resolveConfig(makeBru({
      cwd: ROOT,
      envVars: { ...baseEnv, kafkaSslEnabled: 'true', kafkaCertsDir: tmp, kafkaTruststore: 'nao-existe.jks' },
      vars: { kafkaTopic: 't' },
    }).bru),
    /nao encontrado.*nao-existe\.jks/s
  ));

  await check('SASL ligado sem senha -> avisa', () => expectThrows(
    () => resolveConfig(makeBru({
      cwd: ROOT,
      envVars: { ...baseEnv, kafkaSaslEnabled: 'true', kafkaSaslUsername: 'u' },
      vars: { kafkaTopic: 't' },
    }).bru),
    /kafkaSaslPassword/
  ));

  console.log('\n== trava de producao ==');

  await check('environment de producao bloqueia o envio', async () => {
    const { bru } = makeBru({
      cwd: ROOT, envName: 'prod',
      envVars: { ...baseEnv, kafkaIsProduction: 'true', kafkaAllowProduction: 'false' },
      vars: { kafkaTopic: 't' },
    });
    await expectThrows(
      () => brunoKafka.produce({ req: makeReq({ body: { a: 1 } }), bru }),
      /PRODUCAO BLOQUEADA/
    );
  });

  await check('nome do environment "prd" tambem trava (sem a flag)', async () => {
    const { bru } = makeBru({
      cwd: ROOT, envName: 'prd-brasil',
      envVars: { ...baseEnv }, vars: { kafkaTopic: 't' },
    });
    await expectThrows(() => brunoKafka.produce({ req: makeReq({ body: {} }), bru }), /PRODUCAO BLOQUEADA/);
  });

  await check('kafkaAllowProduction=true libera', async () => {
    const { bru } = makeBru({
      cwd: ROOT, envName: 'prod',
      envVars: { ...baseEnv, kafkaIsProduction: 'true', kafkaAllowProduction: 'true', kafkaDryRun: 'true' },
      vars: { kafkaTopic: 't' },
    });
    const result = await brunoKafka.produce({ req: makeReq({ body: { a: 1 } }), bru });
    assert(result.dryRun === true);
  });

  console.log('\n== payload: body, headers, key, tombstone ==');

  await check('body json + interpolacao de {{var}}', async () => {
    const { bru, state } = makeBru({
      cwd: ROOT,
      envVars: { ...baseEnv, kafkaDryRun: 'true' },
      vars: { kafkaTopic: 't', orderId: 'ABC-9' },
    });
    const req = makeReq({ body: { orderId: '{{orderId}}' }, headers: { 'content-type': 'application/json', 'tipo-evento': 'X' } });
    await brunoKafka.produce({ req, bru });
    assert(state.kafkaSendStatus === 'dryrun', 'status errado: ' + state.kafkaSendStatus);
  });

  await check('body vazio vira tombstone (value null)', async () => {
    const { bru } = makeBru({ cwd: ROOT, envVars: { ...baseEnv, kafkaDryRun: 'true' }, vars: { kafkaTopic: 't' } });
    const result = await brunoKafka.produce({ req: makeReq({ body: '' }), bru });
    assert(result.bytes === 0, 'tombstone deveria ter 0 bytes');
  });

  await check('headers de HTTP nao viram headers Kafka', async () => {
    let captured;
    const { bru } = makeBru({ cwd: ROOT, envVars: { ...baseEnv, kafkaDryRun: 'true' }, vars: { kafkaTopic: 't' } });
    const req = makeReq({ body: { a: 1 }, headers: { 'content-type': 'application/json', 'accept': '*/*', 'correlation-id': 'c-1' } });
    const originalLog = console.log;
    console.log = (msg) => { captured = String(msg); };
    await brunoKafka.produce({ req, bru });
    console.log = originalLog;
    assert(/headers.*correlation-id/.test(captured), 'correlation-id deveria aparecer');
    assert(!/content-type/.test(captured), 'content-type nao deveria virar header Kafka');
  });

  console.log('\n== Schema Registry (mock HTTP real) ==');

  const registry = await startMockRegistry({ 'pedidos-value': AVRO });

  await check('dry run codifica em Avro pelo ultimo schema do subject', async () => {
    const { bru } = makeBru({
      cwd: ROOT,
      envVars: {
        ...baseEnv, kafkaDryRun: 'true',
        kafkaSchemaRegistryEnabled: 'true', kafkaSchemaRegistryUrl: registry.url,
      },
      vars: { kafkaTopic: 'pedidos' },
    });
    const req = makeReq({ body: { orderId: '12345', status: 'created', amount: 199.9 } });
    const result = await brunoKafka.produce({ req, bru });
    assert(result.schemaId === 1, 'schemaId errado: ' + result.schemaId);
    assert(result.bytes > 5, 'payload avro vazio');
  });

  await check('payload que nao bate com o schema falha ANTES de publicar', async () => {
    const { bru } = makeBru({
      cwd: ROOT,
      envVars: { ...baseEnv, kafkaDryRun: 'true', kafkaSchemaRegistryEnabled: 'true', kafkaSchemaRegistryUrl: registry.url },
      vars: { kafkaTopic: 'pedidos' },
    });
    await expectThrows(
      () => brunoKafka.produce({ req: makeReq({ body: { orderId: 12345 } }), bru }),
      /invalid|string|amount|status/i
    );
  });

  await check('kafkaSchemaInline registra e reaproveita o mesmo id', async () => {
    const mk = () => makeBru({
      cwd: ROOT,
      envVars: {
        ...baseEnv, kafkaDryRun: 'true',
        kafkaSchemaRegistryEnabled: 'true', kafkaSchemaRegistryUrl: registry.url,
        kafkaSchemaInline: AVRO,
      },
      vars: { kafkaTopic: 'novo-topico' },
    }).bru;
    const body = { orderId: '1', status: 'created', amount: 1.5 };
    const a = await brunoKafka.produce({ req: makeReq({ body }), bru: mk() });
    const b = await brunoKafka.produce({ req: makeReq({ body }), bru: mk() });
    assert(a.schemaId === b.schemaId, 'schema duplicado no registry');
  });

  await check('kafkaSchemaFile aponta para arquivo versionado', async () => {
    const { bru } = makeBru({
      cwd: ROOT,
      envVars: {
        ...baseEnv, kafkaDryRun: 'true',
        kafkaSchemaRegistryEnabled: 'true', kafkaSchemaRegistryUrl: registry.url,
        kafkaSchemaFile: 'schemas/pagamentos.pedido.criado.v1-value.avsc',
      },
      vars: { kafkaTopic: 'do-arquivo' },
    });
    const result = await brunoKafka.produce({
      req: makeReq({ body: { orderId: '1', status: 'created', amount: 2 } }), bru,
    });
    assert(result.schemaId > 0);
  });

  await check('kafkaSchemaFile inexistente -> erro nomeia a variavel', () => expectThrows(
    () => resolveConfig(makeBru({
      cwd: ROOT,
      envVars: { ...baseEnv, kafkaSchemaRegistryEnabled: 'true', kafkaSchemaRegistryUrl: registry.url, kafkaSchemaFile: 'schemas/sumiu.avsc' },
      vars: { kafkaTopic: 't' },
    }).bru),
    /kafkaSchemaFile/
  ));

  await check('subject inexistente -> erro do registry chega legivel', async () => {
    const { bru } = makeBru({
      cwd: ROOT,
      envVars: { ...baseEnv, kafkaDryRun: 'true', kafkaSchemaRegistryEnabled: 'true', kafkaSchemaRegistryUrl: registry.url },
      vars: { kafkaTopic: 'topico-sem-schema' },
    });
    await expectThrows(() => brunoKafka.produce({ req: makeReq({ body: { a: 1 } }), bru }), /not found|40401/i);
  });

  console.log('\n== erros de rede: mensagem + causa provavel ==');

  await check('broker inacessivel -> erro com dica de causa', async () => {
    const { bru } = makeBru({ cwd: ROOT, envVars: baseEnv, vars: { kafkaTopic: 't' } });
    const err = await expectThrows(
      () => brunoKafka.produce({ req: makeReq({ body: { a: 1 } }), bru }),
      /Causa provavel/
    );
    assert(/broker|escutando|VPN/i.test(err.message), 'dica pouco util: ' + err.message);
  }).catch(() => {});

  await check('consumer com broker inacessivel falha limpo', async () => {
    const { bru } = makeBru({ cwd: ROOT, envVars: baseEnv, vars: { kafkaTopic: 't' } });
    await expectThrows(() => brunoKafka.consume({ bru }), /Causa provavel|offsets|topico/i);
  });

  console.log('\n== doctor ==');

  await check('doctor sem brokers para no primeiro item, sem stack trace', async () => {
    const { checks, ok } = await runDiagnostics({ connection: { brokers: [], ssl: {}, sasl: {} } });
    assert(ok === false);
    assert(checks[checks.length - 1].name === 'Variaveis de conexao');
    assert(/kafkaBrokers vazio/.test(checks[checks.length - 1].detail));
  });

  await check('doctor reporta keystore ilegivel sem derrubar o resto', async () => {
    const { checks } = await runDiagnostics({
      connection: {
        brokers: ['127.0.0.1:59092'],
        ssl: { enabled: true, truststore: { path: pemPath }, keystore: { path: jksPath } },
        sasl: {},
      },
    });
    const trust = checks.find((c) => c.name === 'Certificado: truststore');
    const key = checks.find((c) => c.name === 'Certificado: keystore');
    assert(trust.status === 'ok', 'pem deveria carregar');
    assert(key.status === 'fail' && /senha/i.test(key.detail + key.hint), 'jks sem senha deveria falhar com dica');
    assert(checks.some((c) => c.name === 'Conexao com o cluster' && c.status === 'fail'), 'deveria tentar conectar mesmo assim');
  });

  await check('doctor valida o Schema Registry de verdade', async () => {
    const { checks } = await runDiagnostics({
      topic: 'pedidos',
      connection: { brokers: ['127.0.0.1:59092'], ssl: {}, sasl: {} },
      schemaRegistry: { enabled: true, url: registry.url, value: { subject: 'pedidos-value' } },
    });
    const sr = checks.find((c) => c.name === 'Schema Registry');
    const subject = checks.find((c) => c.name === 'Subject do topico');
    assert(sr.status === 'ok', 'registry deveria responder: ' + sr.detail);
    assert(subject.status === 'ok' && /versao 1/.test(subject.detail), 'subject: ' + subject.detail);
  });

  await check('doctor avisa quando o subject ainda nao existe', async () => {
    const { checks } = await runDiagnostics({
      topic: 'inexistente',
      connection: { brokers: ['127.0.0.1:59092'], ssl: {}, sasl: {} },
      schemaRegistry: { enabled: true, url: registry.url, value: { subject: 'inexistente-value' } },
    });
    const subject = checks.find((c) => c.name === 'Subject do topico');
    assert(subject.status === 'warn' && /Subject not found|nao existe/i.test(subject.detail + subject.hint));
  });

  console.log('\n== consumidor: onde comecar a ler e como decodificar ==');

  const { planPartitions, decodePayload, decodeHeaders } = require(path.join(ROOT, 'lib', 'kafka-consumer.js'));

  await check('pega as ultimas N de uma particao cheia', () => {
    const [p] = planPartitions([{ partition: 0, low: '0', high: '100' }], 10);
    assert(p.start === 90 && p.pending === 10, JSON.stringify(p));
  });

  await check('particao com menos mensagens que N nao volta antes do inicio', () => {
    const [p] = planPartitions([{ partition: 0, low: '0', high: '3' }], 10);
    assert(p.start === 0 && p.pending === 3, JSON.stringify(p));
  });

  await check('respeita a retencao (low > 0)', () => {
    const [p] = planPartitions([{ partition: 0, low: '50', high: '52' }], 10);
    assert(p.start === 50 && p.pending === 2, JSON.stringify(p));
  });

  await check('particao vazia nao gera leitura', () => {
    const [p] = planPartitions([{ partition: 0, low: '7', high: '7' }], 10);
    assert(p.pending === 0, JSON.stringify(p));
  });

  await check('fromBeginning le desde o inicio da retencao', () => {
    const [p] = planPartitions([{ partition: 0, low: '5', high: '100' }], 10, true);
    assert(p.start === 5 && p.pending === 95, JSON.stringify(p));
  });

  await check('round-trip: o que o producer codifica em Avro, o consumer le de volta', async () => {
    const { createRegistry, encodeWithSchemaRegistry } = require(path.join(ROOT, 'lib', 'kafka-producer.js'));
    const client = createRegistry({ url: registry.url });
    const payload = { orderId: '99', status: 'created', amount: 42.5 };
    const { buffer } = await encodeWithSchemaRegistry(client, { subject: 'pedidos-value' }, 'pedidos-value', payload);
    assert(buffer[0] === 0, 'faltou o magic byte do formato Confluent');
    const decoded = await decodePayload(buffer, client);
    assert(JSON.stringify(decoded) === JSON.stringify(payload), 'round-trip perdeu dados: ' + JSON.stringify(decoded));
  });

  await check('payload JSON puro (sem registry) tambem e legivel', async () => {
    const decoded = await decodePayload(Buffer.from('{"a":1}'), undefined);
    assert(decoded.a === 1);
  });

  await check('headers em Buffer viram texto', () => {
    const headers = decodeHeaders({ 'correlation-id': Buffer.from('c-1'), origem: 'bruno' });
    assert(headers['correlation-id'] === 'c-1' && headers.origem === 'bruno');
  });

  await registry.close();
  summary();
})().catch((err) => { console.error('ERRO NO HARNESS:', err); process.exit(1); });
