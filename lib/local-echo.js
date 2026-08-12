/**
 * local-echo.js
 * -------------
 * O Bruno SEMPRE dispara a requisicao HTTP do request quando voce clica em
 * "Send" - nao existe request "so script". Em vez de mandar essa chamada para
 * um servico externo (a versao antiga usava httpbin.org, o que fazia o payload
 * real sair da maquina), esta collection responde a si mesma: o script sobe um
 * servidor HTTP efemero em 127.0.0.1, aponta o request para ele e devolve o
 * resultado do Kafka. Assim a aba Response mostra 200 verde com particao,
 * offset e tempo, e nenhum byte sai da maquina.
 *
 * Camadas de seguranca (nesta ordem):
 *   1. escuta SO em 127.0.0.1 (nunca 0.0.0.0), em porta efemera sorteada pelo SO
 *   2. o caminho da URL e um token aleatorio de 192 bits: quem nao souber o
 *      token recebe 404 sem nenhum dado
 *   3. recusa conexao que nao venha do proprio loopback
 *   4. uso unico: fecha depois da primeira resposta valida
 *   5. morre sozinho no timeout, mesmo que ninguem conecte, e o handle e
 *      unref() para nunca segurar o processo do Bruno vivo
 *   6. `assertLoopbackUrl` barra o envio ANTES de publicar se a URL do request
 *      tiver sido apontada para fora da maquina
 */

const http = require('http');
const crypto = require('crypto');

const LOOPBACK_NAMES = new Set(['localhost', '127.0.0.1', '::1']);

/** Aceita localhost, ::1 e toda a faixa 127.0.0.0/8. */
function isLoopbackHost(host) {
  if (!host) return false;
  const clean = String(host).trim().toLowerCase()
    .replace(/^\[/, '').replace(/\]$/, '')   // [::1] -> ::1
    .replace(/^::ffff:/, '');                // ::ffff:127.0.0.1 -> 127.0.0.1
  if (LOOPBACK_NAMES.has(clean)) return true;
  return /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(clean);
}

/**
 * Barra o envio se a URL do request nao for local. Roda ANTES de publicar no
 * Kafka: se alguem (ou um environment antigo) apontar o request para um
 * endpoint externo, o payload nao chega nem a ser montado.
 */
function assertLoopbackUrl(rawUrl) {
  const url = String(rawUrl === undefined || rawUrl === null ? '' : rawUrl).trim();

  if (!url) return; // sem URL nao ha para onde vazar

  if (url.includes('{{')) {
    throw new Error(
      `BLOQUEADO POR SEGURANCA: a URL do request ("${url}") tem uma variavel que nao foi ` +
      'resolvida, entao nao da para saber para onde a chamada HTTP iria.\n' +
      'Deixe a URL do request como "http://127.0.0.1:1/eco-local" - o script troca ' +
      'por um eco local na hora do envio.'
    );
  }

  let parsed;
  try {
    parsed = new URL(url);
  } catch (err) {
    throw new Error(`BLOQUEADO POR SEGURANCA: URL do request invalida ("${url}").`);
  }

  if (!isLoopbackHost(parsed.hostname)) {
    throw new Error(
      `BLOQUEADO POR SEGURANCA: a URL do request aponta para "${parsed.host}", fora desta maquina, ` +
      'e o corpo dessa chamada seria o seu payload.\n' +
      'Esta collection publica no Kafka pelo script; a chamada HTTP e apenas um eco local. ' +
      'Volte a URL para "http://127.0.0.1:1/eco-local".'
    );
  }
}

/**
 * Sobe um servidor de uso unico em 127.0.0.1 que responde `payload` em JSON
 * para quem acertar o token, e devolve a URL completa para apontar o request.
 *
 * @param {object} payload      o que aparecera na aba Response do Bruno
 * @param {object} [opts]
 * @param {number} [opts.timeoutMs=5000]  tempo maximo de vida do servidor
 * @returns {Promise<{url:string, port:number, token:string, close:Function}>}
 */
function serveOnce(payload, opts) {
  const { timeoutMs = 5000 } = opts || {};
  const token = crypto.randomBytes(24).toString('hex');
  const body = Buffer.from(JSON.stringify(payload, null, 2), 'utf-8');

  return new Promise((resolve, reject) => {
    let timer;

    const shutdown = () => {
      if (timer) clearTimeout(timer);
      try { server.close(); } catch (err) { /* ja fechado */ }
    };

    const server = http.createServer((req, res) => {
      const remote = String(req.socket.remoteAddress || '');
      const path = String(req.url || '').split('?')[0];
      const allowed = isLoopbackHost(remote) && path === `/${token}`;

      if (!allowed) {
        // Nunca entrega o payload, e NAO derruba o servidor: quem tem o token
        // ainda precisa conseguir buscar o resultado.
        res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
        res.end('{"erro":"nao encontrado"}');
        return;
      }

      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Content-Length': body.length,
        'Cache-Control': 'no-store',
        Connection: 'close',
      });
      res.end(body, shutdown);
    });

    server.on('error', (err) => {
      if (timer) clearTimeout(timer);
      reject(err);
    });

    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      timer = setTimeout(shutdown, timeoutMs);
      if (typeof timer.unref === 'function') timer.unref();
      if (typeof server.unref === 'function') server.unref();
      resolve({ url: `http://127.0.0.1:${port}/${token}`, port, token, close: shutdown });
    });
  });
}

module.exports = { serveOnce, assertLoopbackUrl, isLoopbackHost };
