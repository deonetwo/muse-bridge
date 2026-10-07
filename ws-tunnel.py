#!/usr/bin/env python3
"""Klien terowongan WebSocket manual (tanpa library websockets).

Menghubungkan wss://<host>/tunnel ke bridge lokal 127.0.0.1:8765.
Implementasi WebSocket minimal: handshake HTTP Upgrade + frame sederhana.
"""
import base64
import hashlib
import http.client
import json
import logging
import os
import secrets
import socket
import ssl
import struct
import threading
import time

LOG = logging.getLogger('ws-tunnel-manual')

BRIDGE_HOST = os.environ.get('TUNNEL_BRIDGE_HOST', '127.0.0.1')
BRIDGE_PORT = int(os.environ.get('TUNNEL_BRIDGE_PORT', '8765'))
WS_HOST = os.environ.get('TUNNEL_WS_HOST', 'muse.example.com')
WS_PATH = '/tunnel'
TUNNEL_SECRET = os.environ.get('TUNNEL_SECRET', '')

PROXY_HOST = 'YOUR_EGRESS_PROXY'
PROXY_PORT = 3128


def ws_connect():
    """Buka koneksi WebSocket via proxy. Kembalikan socket yang sudah handshake."""
    # 1. TCP ke proxy
    s = socket.create_connection((PROXY_HOST, PROXY_PORT), timeout=15)
    # 2. CONNECT
    s.sendall(f"CONNECT {WS_HOST}:443 HTTP/1.1\r\nHost: {WS_HOST}:443\r\n\r\n".encode())
    resp = b''
    while b'\r\n\r\n' not in resp:
        chunk = s.recv(4096)
        if not chunk:
            raise RuntimeError('proxy menutup saat CONNECT')
        resp += chunk
    if b'200' not in resp.split(b'\r\n', 1)[0]:
        raise RuntimeError(f'CONNECT gagal: {resp[:60]}')
    # 3. TLS
    ctx = ssl.create_default_context()
    t = ctx.wrap_socket(s, server_hostname=WS_HOST)
    # 4. HTTP Upgrade ke WebSocket
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    req = (f"GET {WS_PATH} HTTP/1.1\r\n"
           f"Host: {WS_HOST}\r\n"
           f"Upgrade: websocket\r\n"
           f"Connection: Upgrade\r\n"
           f"Sec-WebSocket-Key: {key}\r\n"
           f"Sec-WebSocket-Version: 13\r\n"
           f"X-Tunnel-Secret: {TUNNEL_SECRET}\r\n\r\n")
    t.sendall(req.encode())
    resp = b''
    while b'\r\n\r\n' not in resp:
        chunk = t.recv(4096)
        if not chunk:
            raise RuntimeError('server menutup saat upgrade')
        resp += chunk
    if b'101' not in resp.split(b'\r\n', 1)[0]:
        raise RuntimeError(f'upgrade gagal: {resp[:120]}')
    LOG.info('WebSocket tersambung')
    return t


def ws_send(sock, data):
    """Kirim satu text frame (masked, sesuai spec client)."""
    if isinstance(data, str):
        data = data.encode()
    mask = secrets.token_bytes(4)
    header = bytes([0x81])
    ln = len(data)
    if ln < 126:
        header += struct.pack('>B', 0x80 | ln)
    elif ln < 65536:
        header += struct.pack('>BH', 0x80 | 126, ln)
    else:
        header += struct.pack('>BQ', 0x80 | 127, ln)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    sock.sendall(header + mask + masked)


def ws_recv(sock):
    """Baca satu frame. Kembalikan (opcode, payload). None jika tutup."""
    hdr = sock.recv(2)
    if len(hdr) < 2:
        return None, None
    b1, b2 = hdr
    opcode = b1 & 0x0F
    ln = b2 & 0x7F
    if ln == 126:
        ln = struct.unpack('>H', sock.recv(2))[0]
    elif ln == 127:
        ln = struct.unpack('>Q', sock.recv(8))[0]
    if b2 & 0x80:
        mask = sock.recv(4)
    else:
        mask = None
    payload = b''
    while len(payload) < ln:
        chunk = sock.recv(min(65536, ln - len(payload)))
        if not chunk:
            break
        payload += chunk
    if mask:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    if opcode == 0x8:  # close
        return None, None
    if opcode == 0x9:  # ping -> pong
        # Kirim pong (sederhana, tanpa mask untuk server? spec: client harus mask)
        return 'ping', payload
    return opcode, payload


def send_pong(sock, payload):
    mask = secrets.token_bytes(4)
    header = bytes([0x8A, 0x80 | len(payload)])
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    try:
        sock.sendall(header + mask + masked)
    except Exception:
        pass


def forward_to_bridge(msg):
    """Teruskan request ke bridge lokal. Kembalikan list pesan balasan."""
    req_id = msg['id']
    body = base64.b64decode(msg.get('body') or b'')
    headers = dict(msg.get('headers') or {})
    conn = http.client.HTTPConnection(BRIDGE_HOST, BRIDGE_PORT, timeout=170)
    out = []
    try:
        conn.request(msg['method'], msg['path'], body=body, headers=headers)
        resp = conn.getresponse()
        rheaders = {}
        for k, v in resp.getheaders():
            if k.lower() in ('connection', 'transfer-encoding', 'content-length'):
                continue
            rheaders[k] = v
        out.append(json.dumps({'id': req_id, 'type': 'headers',
                               'status': resp.status, 'headers': rheaders}))
        while True:
            data = resp.read(65536)
            if not data:
                break
            out.append(json.dumps({'id': req_id, 'type': 'chunk',
                                   'data': base64.b64encode(data).decode()}))
        out.append(json.dumps({'id': req_id, 'type': 'end'}))
    except Exception as e:
        out.append(json.dumps({'id': req_id, 'type': 'error', 'error': str(e)[:300]}))
    finally:
        conn.close()
    return out


def send_ping(sock):
    """Kirim ping frame (masked)."""
    mask = secrets.token_bytes(4)
    header = bytes([0x89, 0x80])
    try:
        sock.sendall(header + mask)
    except Exception:
        pass


def run_once():
    sock = ws_connect()
    # Tanpa read timeout: koneksi idle itu normal. Keepalive via ping berkala.
    sock.settimeout(None)
    send_lock = threading.Lock()
    stop_ping = threading.Event()

    def ping_loop():
        while not stop_ping.wait(25):
            with send_lock:
                send_ping(sock)
    threading.Thread(target=ping_loop, daemon=True).start()

    try:
        while True:
            opcode, payload = ws_recv(sock)
            if opcode is None:
                LOG.warning('server menutup koneksi')
                break
            if opcode == 'ping':
                send_pong(sock, payload)
                continue
            if opcode != 0x1:  # hanya text frame
                continue
            try:
                msg = json.loads(payload)
            except Exception:
                continue
            if msg.get('type') != 'request':
                continue

            def handle(m=msg):
                for out in forward_to_bridge(m):
                    with send_lock:
                        try:
                            ws_send(sock, out)
                        except Exception as e:
                            LOG.warning('gagal kirim: %s', e)
                            return
            threading.Thread(target=handle, daemon=True).start()
    finally:
        stop_ping.set()
        try:
            sock.close()
        except Exception:
            pass


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    if not TUNNEL_SECRET:
        LOG.error('TUNNEL_SECRET kosong')
        raise SystemExit(1)
    backoff = 5
    while True:
        try:
            LOG.info('menghubungkan ke wss://%s%s ...', WS_HOST, WS_PATH)
            run_once()
            LOG.warning('terowongan terputus')
        except Exception as e:
            LOG.warning('terowongan gagal: %s', e)
        LOG.info('mencoba lagi dalam %ds', backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 120)


if __name__ == '__main__':
    main()
