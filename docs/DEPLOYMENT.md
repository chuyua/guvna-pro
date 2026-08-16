# Deployment

**Status 2026-08-16: the VPS deployment is LIVE and the primary daily route.** Arch local is deferred (Phase 8).

## Target 1 — the VPS VPS (LIVE, primary)

Facts (verified 2026-08-14): Debian 13 trixie, x86_64, 1 core, 967MiB RAM (~281MiB available), 2.0GiB swap, 25G disk (14G free); Docker 29.6.2; already running a TLS panel, the VPS-web-1 (8000), caddy 2.9-alpine. SSH: `ssh the VPS` (user alex).

### Live endpoint

- Gateway: `http://127.0.0.1:20128` on the host (host network mode)
- Public: `https://gateway.example.com:9443` (caddy vhost, auto-TLS)
  - **:9443 not :443** — another TLS service also binds `*:443`; two listeners on 443 cause flaky TLS handshakes. Same reason the old new-api vhost used :9443.

### Layout on the VPS

- `/home/alex/bruvroute/docker-compose.yml` + `.env` (chmod 600 — ADMIN_KEY, API_KEYS, provider keys)
- Named volume `bruvroute_bruvroute-data` mounted at `/data` (SQLite telemetry lives there)
- Container: `bruvroute:latest`, distroless static:nonroot, ~13MB image, `mem_limit: 400m`, `GOMEMLIMIT=256MiB`, restart unless-stopped
- Healthcheck: `/bruvroute -healthcheck -config /config.yaml` (binary probes own /healthz — no shell in image)

### Ops commands

```sh
ssh the VPS
cd /home/alex/bruvroute
docker compose up -d          # start / update
docker compose ps             # status + health
docker logs bruvroute -f      # logs
docker compose up -d --build  # rebuild after a push (needs image transfer, see below)
```

Ship a new image (laptop → the VPS; the VPS is too small to build Go comfortably):

```sh
docker build -t bruvroute:latest -f deploy/Dockerfile .
docker save bruvroute:latest | gzip | ssh the VPS "docker load"
ssh the VPS "cd /home/alex/bruvroute && docker compose up -d"
```

Caddy (vhost at `/home/alex/caddy/Caddyfile`, reload inside container):

```sh
ssh the VPS "docker exec caddy caddy reload --config /etc/caddy/Caddyfile"
```

### Remote observability from the laptop

```sh
export BRUVROUTE_URL=https://gateway.example.com:9443 BRUVROUTE_ADMIN_KEY=...
bruvroute-cli status          # chains, step health, usage table (--json for raw)
bruvroute-cli logs -n 100     # tail the in-memory ring
```

Admin endpoints (`/admin/status`, `/admin/logs`) are admin-key-only; nothing new is exposed on the network.

### Gotchas learned

- the VPS's docker **cannot create bridge networks** (iptables setup fails) → `network_mode: host`
- `docker --env-file` does **not** strip quotes around values (shell `source` does) — unquote before use
- Nonroot (UID 65532) cannot `mkdir /data` at container root — the Dockerfile creates + chowns it in the build stage
- Client disconnects mid-stream are dropped silently (never recorded as upstream 502)

## Target 2 — Arch (local, deferred to Phase 8)

- systemd user unit (`bruvroute.service`), runs as this user, port 20128
- Data dir `~/.bruvroute/` (config override, SQLite telemetry, logs)
- Keys via env / keyring, never in git
- Precedent: the removed `prior-gateway.service` user unit used this exact pattern

### app drop-in facts

`~/.app/config.yaml` currently references a dead provider:

```yaml
provider: "prior-gateway"
base_url: http://127.0.0.1:20128/v1
api_key: REDACTED-DEAD-KEY   # dead — prior-gateway was fully removed 2026-08-14
```

- BruvRoute on port 20128 is a drop-in: only the api_key needs updating
- app rewiring is explicitly out of the current scope (Phase 8)

## Security defaults

- Auth always on (admin key + API keys) — never exposed without it
- Scoped revocable tokens for remote CLI
- TLS via caddy when exposed; localhost-only otherwise
