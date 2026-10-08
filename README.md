# Muse Bridge

A self-hosted, OpenAI-compatible API endpoint for the `muse` model, publicly
accessible at `https://muse.example.com/v1`. Built for personal use across
any client that supports custom OpenAI endpoints — Kilo Code, terminal chat,
web UIs, or plain `curl`. Supports text chat (including streaming) and
AI-generated images.

## How it works

```
Client (Kilo Code / terminal / web UI / SDK)
  └─▶ https://muse.example.com/v1/*        Cloudflare Worker `muse-bridge`
        └─▶ Durable Object `TunnelCoordinator`  single global coordinator
              └─▶ wss://muse.example.com/tunnel
                    WebSocket opened FROM the VPS (traverses NAT, no inbound port)
                    └─▶ bridge.py on 127.0.0.1:8765
                          └─▶ file queue: pending/ → processing/ → done/
                                └─▶ worker (cron, every minute) answers as Muse
```

Why not `cloudflared`? The VPS egress proxy performs TLS interception, which
breaks cloudflared's data plane. The Worker + hand-rolled WebSocket client
(`bin/ws-tunnel.py`, stdlib only) achieves the same result over plain HTTPS
egress.

## Features

- **OpenAI-compatible** — `/v1/chat/completions` (progressive streaming via
  SSE), `/v1/models`; works with the official OpenAI SDKs by overriding
  `base_url`.
- **Image generation & serving** — ask for an image, get a signed markdown URL.
- **File generation & serving** — ask for a file (code, doc, etc.), get a
  signed download URL.
- **Signed URLs** — image/file URLs carry HMAC signatures with expiry;
  unsigned URLs are rejected (anti-scraping).
- **Role-based API keys** — one revocable key per app/instance; worker and user
  roles are strictly isolated.
- **Resilient tunnel** — auto-reconnect with backoff plus a 25s ping keepalive.
- **Self-healing ops** — a watchdog cron reinstalls systemd units and restarts
  services if the VM wipes `/etc/systemd/system` on reboot (it does).
- **Free tier** — Cloudflare Workers free plan (100k req/day); no 9Router,
  no extra services.

## Repository layout

```
.
├── README.md                  this file
├── PROMPT.md                  the mega-prompt: worker instructions + env +
│                              systemd units + ops (everything to run it)
├── bridge.py                  HTTP server (Python stdlib only)
├── muse-bridge-do.js          Cloudflare Worker source (Durable Object)
└── ws-tunnel.py               outbound WebSocket tunnel client (stdlib only)
```

Runtime files created on the VPS (not in git): `bridge.env`,
`ws-tunnel.env`, `*.service` units (embedded in PROMPT.md), `keys.json`,
`queue/`, `images/`, `files/`, client `.env` files.

## Public API

Base URL: `https://muse.example.com/v1`
Authentication: `Authorization: Bearer <api-key>` (except `/health`).

| Endpoint | Auth | Notes |
|---|---|---|
| `GET /v1/models` | user | Returns `{"id": "muse", …}` |
| `POST /v1/chat/completions` | user | `stream: true` → progressive SSE chunks, ends with `data: [DONE]` |
| `GET /v1/images/<id>[.ext]` | signed URL | PNG/JPG/WebP/GIF; requires `?expires=&sig=` |
| `GET /v1/files/<id>[.ext]` | signed URL | txt/md/py/html/pdf/zip/…; requires `?expires=&sig=` |
| `GET /health` | none | `{"ok": true}` |
| `/muse/*` | — | **Not exposed** (Worker returns 404) |
| `/tunnel` | WS secret | Tunnel endpoint for the VPS client only |

### Quick test

```bash
curl -s https://muse.example.com/v1/chat/completions \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"muse","messages":[{"role":"user","content":"Hello!"}]}'
```

```python
from openai import OpenAI
client = OpenAI(base_url="https://muse.example.com/v1", api_key="...")
r = client.chat.completions.create(
    model="muse",
    messages=[{"role": "user", "content": "Hello!"}],
)
print(r.choices[0].message.content)
```

## API keys

One revocable key per application/instance. Keys are written to a `600` file
and **never printed** when `--out` is used.

```bash
export BRIDGE_KEYS=/home/user/muse-bridge/keys.json
export BRIDGE_QUEUE=/home/user/muse-bridge/queue

# create (key goes straight to file, not stdout)
python3 bridge.py keygen --role user --label my-app \
  --out /home/user/muse-forward/my-app.env \
  --base-url https://muse.example.com/v1

python3 bridge.py keylist        # labels, roles, key prefixes only
python3 bridge.py keydel my-app   # revoke
```

Each `--out` file contains `OPENAI_BASE_URL`, `OPENAI_API_KEY`, `OPENAI_MODEL`.

### Example client setup (Kilo Code)

- API Provider: **OpenAI Compatible**
- Base URL / API Key / Model: from the `.env` file (`muse`)
- Context window: `128000` · Max output tokens: `8192` · Request timeout: `> 240s`

## Images & files (signed URLs)

When the user asks for an image, the worker generates it, saves it as
`images/<uuid>.png`, requests a signed URL from the bridge, and replies with:

```markdown
![description](https://muse.example.com/v1/images/<uuid>.png?expires=...&sig=...)
```

When the user asks for a file, the worker saves it as `files/<uuid>.<ext>`
and replies with:

```markdown
[filename.ext](https://muse.example.com/v1/files/<uuid>.ext?expires=...&sig=...)
```

**Security:** image/file URLs require a valid HMAC signature (`?expires=&sig=`).
Unsigned or expired URLs return 403. Signatures are issued via the internal
`POST /muse/sign_url` endpoint (worker role only) with a configurable TTL
(default 1 hour, max 7 days). The signing secret (`BRIDGE_URL_SECRET`) lives
in `bridge.env` (mode 600) and never leaves the server. UUIDs remain
unguessable as a second layer; there is no directory listing.

## Deploying the Cloudflare Worker

Prerequisites: a Cloudflare API token with **Workers Scripts: Edit** on the
account, plus the tunnel secret.

```bash
ACCT=<account-id>  SECRET=$(cat ~/.cloudflared/.tunnel-secret)
sed "s/REPLACE_TUNNEL_SECRET/$SECRET/" muse-bridge-do.js > /tmp/w.js

# multipart upload with Durable Object binding (SQLite class, free-plan OK)
python3 - <<'EOF'
import json, secrets
meta = {
  "main_module": "worker.js",
  "bindings": [{"type": "durable_object_namespace",
                "name": "TUNNEL_DO", "class_name": "TunnelCoordinator"}],
  "migrations": {"new_classes": [], "new_sqlite_classes": ["TunnelCoordinator"],
                 "deleted_classes": [], "renamed_classes": []},
  "compatibility_date": "2024-01-01",
}
# ... build multipart body with metadata + worker.js, PUT to
# https://api.cloudflare.com/client/v4/accounts/$ACCT/workers/scripts/muse-bridge
EOF
```

Then create the route `muse.example.com/*` → script `muse-bridge` and a
proxied `AAAA muse.example.com → 100::` record (dummy target; the Worker
handles the traffic).

## Operations

```bash
systemctl status muse-bridge ws-tunnel
journalctl -u ws-tunnel -n 20 --no-pager   # tunnel connectivity
curl -s http://127.0.0.1:8765/health       # local bridge
curl -s https://muse.example.com/health  # full public chain
```

> **Reboot caveat:** `/etc/systemd/system/` on this VM is ephemeral and is
> wiped on every reboot. The canonical units live in the repo
> (`muse-bridge.service`, `ws-tunnel.service`). A watchdog cron (every 5 min)
> reinstalls missing units and restarts the services automatically.

## Security model

- The bridge binds **only** `127.0.0.1` — nothing is exposed directly.
- Only `/v1/*`, `/health`, and `/tunnel` (secret-guarded) are reachable
  publicly; `/muse/*` (internal worker API) returns 404 at the edge.
- Role isolation enforced in code: a `user` key gets 401 on `/muse/*`,
  a `worker` key gets 401 on `/v1/*`.
- Credential files are mode `600`; keys are never echoed in chat or logs.
- If a key leaks: `bridge.py keydel <label>` revokes it instantly.

## Known limitations

- Answers take ~10–20s (1-minute worker poll + queue). Streaming delivers
  text progressively once generation starts, but time-to-first-token is
  unchanged.
- OpenAI `tools`/`tool_calls` (function calling) is **not** implemented —
  agentic IDE modes won't work; chat, streaming, images, and files do.
- The WS tunnel can drop during long idle periods; the client auto-reconnects.

## Changelog

- **2026-10-08** — Signed URLs for `/v1/images/*` and `/v1/files/*`
  (`?expires=&sig=`, via `POST /muse/sign_url`); unsigned URLs → 403.
- **2026-10-08** — Added `/v1/files/*` for general file delivery
  (code, docs, archives) with MIME-type handling.
- **2026-10-07** — Progressive streaming: `/muse/answer_chunk` for real-time
  SSE deltas; bridge falls back to sentence-by-sentence delivery.
- **2026-10-07** — Rebuilt in DIRECT mode (no 9Router/Hermes). Replaced
  `cloudflared` (broken by egress TLS interception) with Worker + WebSocket.
- **2026-10-07** — Added `/v1/images/*`; worker is now general-purpose
  (previously Kilo Code text-only).
