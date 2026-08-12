/**
 * http-json.js
 * ------------
 * Cliente HTTP minimo (sem dependencia externa) usado para falar com o Schema
 * Registry de forma controlada - tanto no diagnostico quanto no registro de
 * schema. Suporta basic auth, agent TLS proprio (para CA interna) e timeout.
 */

const http = require('http');
const https = require('https');
const { URL } = require('url');

/**
 * @param {string} url
 * @param {object} [opts]
 * @param {string} [opts.method='GET']
 * @param {object} [opts.body]           serializado como JSON
 * @param {string} [opts.contentType]
 * @param {object} [opts.auth]           { username, password }
 * @param {object} [opts.agent]          https.Agent (CA interna / mTLS)
 * @param {number} [opts.timeoutMs=10000]
 * @returns {Promise<any>} corpo da resposta ja parseado (ou texto cru)
 */
function requestJson(url, opts = {}) {
  const {
    method = 'GET', body, contentType = 'application/json',
    auth, agent, timeoutMs = 10000,
  } = opts;

  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const client = parsed.protocol === 'https:' ? https : http;
    const payload = body === undefined ? undefined : JSON.stringify(body);

    const headers = { Accept: 'application/vnd.schemaregistry.v1+json, application/json' };
    if (payload !== undefined) {
      headers['Content-Type'] = contentType;
      headers['Content-Length'] = Buffer.byteLength(payload);
    }
    if (auth && auth.username) {
      headers.Authorization = 'Basic ' + Buffer.from(`${auth.username}:${auth.password || ''}`).toString('base64');
    }

    const req = client.request(parsed, { method, headers, agent, timeout: timeoutMs }, (res) => {
      let text = '';
      res.on('data', (chunk) => { text += chunk; });
      res.on('end', () => {
        let data = text;
        try { data = JSON.parse(text); } catch (err) { /* resposta nao-JSON: devolve texto */ }
        if (res.statusCode >= 400) {
          const detail = (data && data.message) || (typeof data === 'string' ? data.slice(0, 300) : JSON.stringify(data));
          return reject(new Error(`HTTP ${res.statusCode} em ${method} ${parsed.pathname}: ${detail}`));
        }
        resolve(data);
      });
    });

    req.on('timeout', () => req.destroy(new Error(`timeout de ${timeoutMs}ms em ${method} ${url}`)));
    req.on('error', reject);
    if (payload !== undefined) req.write(payload);
    req.end();
  });
}

module.exports = { requestJson };
