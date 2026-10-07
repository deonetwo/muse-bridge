/**
 * muse-bridge reverse tunnel worker dengan Durable Object.
 * Durable Object memastikan satu koordinator global untuk semua edge.
 */

const TUNNEL_SECRET = 'REPLACE_TUNNEL_SECRET';

function b64encode(buf) {
  const bytes = new Uint8Array(buf);
  let s = '';
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    s += String.fromCharCode.apply(null, bytes.subarray(i, i + CHUNK));
  }
  return btoa(s);
}

function b64decode(b64) {
  const s = atob(b64);
  const bytes = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) bytes[i] = s.charCodeAt(i);
  return bytes;
}

export class TunnelCoordinator {
  constructor(state) {
    this.state = state;
    this.vmSocket = null;
    this.pending = new Map();
  }

  async fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === '/tunnel') {
      if (req.headers.get('Upgrade') !== 'websocket') {
        return new Response('websocket expected', { status: 400 });
      }
      if (req.headers.get('X-Tunnel-Secret') !== TUNNEL_SECRET) {
        return new Response('unauthorized', { status: 401 });
      }
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();
      if (this.vmSocket) { try { this.vmSocket.close(1000, 'replaced'); } catch (e) {} }
      this.vmSocket = server;
      for (const [, p] of this.pending) {
        try { p.rejectHeaders(new Error('tunnel reconnected')); } catch (e) {}
        try { p.writer.abort(new Error('tunnel reconnected')); } catch (e) {}
      }
      this.pending.clear();
      server.addEventListener('message', (e) => {
        let msg;
        try { msg = JSON.parse(e.data); } catch (err) { return; }
        const p = this.pending.get(msg.id);
        if (!p) return;
        if (msg.type === 'headers') {
          p.resolveHeaders({ status: msg.status, headers: msg.headers || {} });
        } else if (msg.type === 'chunk') {
          p.writer.write(b64decode(msg.data)).catch(() => {});
        } else if (msg.type === 'end') {
          this.pending.delete(msg.id);
          p.writer.close().catch(() => {});
        } else if (msg.type === 'error') {
          this.pending.delete(msg.id);
          const err = new Error(msg.error || 'upstream error');
          p.rejectHeaders(err);
          p.writer.abort(err).catch(() => {});
        }
      });
      const onClose = () => { if (this.vmSocket === server) this.vmSocket = null; };
      server.addEventListener('close', onClose);
      server.addEventListener('error', onClose);
      return new Response(null, { status: 101, webSocket: client });
    }

    const p = url.pathname;
    if (p !== '/health' && !p.startsWith('/v1/')) {
      return new Response('not found', { status: 404 });
    }
    if (!this.vmSocket) {
      return new Response('tunnel offline', { status: 503 });
    }

    const id = crypto.randomUUID();
    const bodyBuf = await req.arrayBuffer();
    const fwdHeaders = {};
    for (const [k, v] of req.headers) {
      const lk = k.toLowerCase();
      if (lk === 'host' || lk === 'connection' || lk === 'content-length') continue;
      fwdHeaders[k] = v;
    }

    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();

    let resolveHeaders, rejectHeaders;
    const headersPromise = new Promise((res, rej) => {
      resolveHeaders = res; rejectHeaders = rej;
    });
    const timer = setTimeout(() => {
      this.pending.delete(id);
      const err = new Error('tunnel timeout');
      rejectHeaders(err);
      writer.abort(err).catch(() => {});
    }, 180000);
    this.pending.set(id, {
      writer,
      resolveHeaders: (v) => { clearTimeout(timer); resolveHeaders(v); },
      rejectHeaders: (e) => { clearTimeout(timer); rejectHeaders(e); },
    });

    try {
      this.vmSocket.send(JSON.stringify({
        id,
        type: 'request',
        method: req.method,
        path: url.pathname + url.search,
        headers: fwdHeaders,
        body: b64encode(bodyBuf),
      }));
    } catch (e) {
      this.pending.delete(id);
      clearTimeout(timer);
      return new Response('tunnel send failed', { status: 502 });
    }

    let head;
    try {
      head = await headersPromise;
    } catch (e) {
      return new Response('tunnel error: ' + e.message, { status: 502 });
    }
    return new Response(readable, { status: head.status, headers: head.headers });
  }
}

export default {
  async fetch(req, env) {
    const id = env.TUNNEL_DO.idFromName('main');
    const stub = env.TUNNEL_DO.get(id);
    return stub.fetch(req);
  },
};
