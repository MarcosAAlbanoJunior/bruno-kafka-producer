/* Harness de teste: simula o objeto `bru`/`req` do Bruno e um Schema Registry. */
const http = require('http');

/**
 * `vars`        simula runtime variables (bru.getVar) - Bruno antigo
 * `requestVars` simula as vars da aba "Vars > Pre Request" das versoes novas,
 *               que SO aparecem em bru.getRequestVar()
 */
function makeBru({ envVars = {}, vars = {}, requestVars, envName = 'local', cwd }) {
  const state = { ...vars };
  const skipped = { value: false };
  const bru = {
    cwd: () => cwd,
    getEnvVar: (n) => envVars[n],
    getVar: (n) => state[n],
    ...(requestVars ? { getRequestVar: (n) => requestVars[n] } : {}),
    setVar: (n, v) => { state[n] = v; },
    getEnvName: () => envName,
    interpolate: (s) => String(s).replace(/\{\{(\w+)\}\}/g, (m, k) => (state[k] !== undefined ? state[k] : (envVars[k] !== undefined ? envVars[k] : m))),
    runner: { skipRequest: () => { skipped.value = true; } },
  };
  return { bru, state, skipped };
}

/** `url` simula o campo URL do request (o eco local troca esse valor). */
function makeReq({ body, headers = {}, url = 'http://127.0.0.1:1/eco-local' }) {
  const estado = { body, url, method: 'POST' };
  return {
    getBody: () => estado.body,
    setBody: (b) => { estado.body = b; },
    getHeaders: () => headers,
    getUrl: () => estado.url,
    setUrl: (u) => { estado.url = u; },
    setMethod: (m) => { estado.method = m; },
    estado,
  };
}

/** GET simples, usado para conversar com o eco local nos testes. */
function httpGet(url) {
  return new Promise((resolve, reject) => {
    const request = http.get(url, (res) => {
      let body = '';
      res.on('data', (c) => { body += c; });
      res.on('end', () => resolve({ status: res.statusCode, body }));
    });
    request.on('error', reject);
    request.setTimeout(3000, () => request.destroy(new Error('timeout')));
  });
}

/* Schema Registry mockado (o suficiente para register/getLatest/getById/encode) */
function startMockRegistry(schemasBySubject) {
  const byId = new Map();
  let nextId = 1;
  const register = (subject, schema) => {
    for (const [id, entry] of byId) if (entry.subject === subject && entry.schema === schema) return id;
    const id = nextId++;
    byId.set(id, { subject, schema, version: byId.size + 1 });
    return id;
  };
  for (const [subject, schema] of Object.entries(schemasBySubject || {})) register(subject, schema);

  const server = http.createServer((req, res) => {
    const send = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/vnd.schemaregistry.v1+json' });
      res.end(JSON.stringify(obj));
    };
    const url = decodeURIComponent(req.url);
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      let m;
      if (req.method === 'GET' && url === '/subjects') {
        return send(200, [...new Set([...byId.values()].map((e) => e.subject))]);
      }
      if ((m = url.match(/^\/schemas\/ids\/(\d+)$/)) && req.method === 'GET') {
        const entry = byId.get(Number(m[1]));
        return entry ? send(200, { schema: entry.schema }) : send(404, { error_code: 40403 });
      }
      if ((m = url.match(/^\/subjects\/(.+)\/versions\/latest$/)) && req.method === 'GET') {
        const found = [...byId.entries()].reverse().find(([, e]) => e.subject === m[1]);
        return found ? send(200, { id: found[0], version: found[1].version, subject: m[1], schema: found[1].schema })
          : send(404, { error_code: 40401, message: `Subject '${m[1]}' not found.` });
      }
      if ((m = url.match(/^\/subjects\/(.+)\/versions$/)) && req.method === 'POST') {
        const parsed = JSON.parse(body);
        return send(200, { id: register(m[1], parsed.schema) });
      }
      send(404, { error_code: 404, message: `nao mockado: ${req.method} ${url}` });
    });
  });

  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve({
      url: `http://127.0.0.1:${server.address().port}`,
      close: () => new Promise((r) => server.close(r)),
    }));
  });
}

/* mini runner de testes */
const results = [];
async function check(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
    console.log(`  ok   ${name}`);
  } catch (err) {
    results.push({ name, ok: false, err });
    console.log(`  FALHA ${name}\n        ${err && err.message}`);
  }
}
function expectThrows(promiseOrFn, matcher) {
  let run;
  try {
    run = typeof promiseOrFn === 'function' ? promiseOrFn() : promiseOrFn;
  } catch (syncErr) {
    run = Promise.reject(syncErr);
  }
  return Promise.resolve(run).then(
    () => { throw new Error('esperava erro, mas passou'); },
    (err) => {
      if (matcher && !matcher.test(err.message)) {
        throw new Error(`erro nao bate com ${matcher}: ${err.message}`);
      }
      return err;
    }
  );
}
function assert(cond, msg) { if (!cond) throw new Error(msg || 'assert falhou'); }
function summary() {
  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} passaram`);
  process.exitCode = failed.length ? 1 : 0;
}

module.exports = { makeBru, makeReq, httpGet, startMockRegistry, check, expectThrows, assert, summary };
