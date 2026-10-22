# Guvna

Low-resource AI gateway in Go. One OpenAI-compatible endpoint that routes to free-tier LLM providers through named, self-healing chains — built for one user, runs in ~15–40MB of RAM.

[![CI](https://github.com/chuyua/guvna-pro/actions/workflows/ci.yml/badge.svg)](https://github.com/chuyua/guvna-pro/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Why

Most LLM API gateways sit at two extremes. Either they're TypeScript bloat — a Node service eating hundreds of MB of RAM to do what is fundamentally forwarding HTTP — or they're enterprise-grade platforms built for teams: multi-tenant, database-backed, dashboard-first, ops-budget-required.

Guvna uses **config-as-code, SQLite telemetry and one static binary** for serving and management, sized for exactly one user.

- **Chain-based routing** — define ordered provider/model fallback steps per use case (`mychain` → bazaarlink qwen → groq llama → mistral codestral). Clients call chain names; real provider model names never leak into your apps.
- **Key pools** — per-provider rotation (round-robin / least-used / sequential) with class-based quarantine (auth / rate-limit / transient) and automatic failover. One dead key never burns the pool.
- **Provider-prefixed passthrough** — call any catalog model directly as `<provider>/<model>` when you don't need a chain.
- **Self-healing** — unhealthy providers cool off (60s → 10min, auto-recovery); chains fail over across steps; no silent mid-stream restarts.
- **Local CLI management** — `guvna status|logs|chains` connects through a local filesystem socket, with the configured admin key. Management has no TCP listener or public API routes.
- **CLI-only management** — no browser login, OAuth flow, web sessions or separate UI service.
- **Observability** — async SQLite telemetry, passive Auto request results and provider availability observations; `guvna status --scoring` displays the collected data.
- **Free-tier native** — ships with configs for Bazaarlink, Groq, Mistral, Gemini, OrcaRouter free tiers.

## Quickstart

```sh
git clone https://github.com/chuyua/guvna-pro && cd guvna-pro
cp .env.example .env        # fill in ADMIN_KEY, API_KEYS, and at least one provider key
docker compose -f deploy/docker-compose.yml up -d
```

Or without Docker:

```sh
go build -o guvna ./cmd/guvna
set -a; source .env; set +a
./guvna serve -config config.yaml  # listens on :20128 (all interfaces)
```

Tagged releases publish the Linux amd64 executable `guvna` and `SHA256SUMS` to
[GitHub Releases](https://github.com/chuyua/guvna-pro/releases), plus the existing
amd64/arm64 image to `ghcr.io/chuyua/guvna-pro`. See [docs/CLI.md](docs/CLI.md)
for download verification. `guvna --version` reports the release tag; source
builds report `dev` unless a version is injected. Existing `guvna -config ...`
service invocations remain supported.

### First request

Create chains with the CLI on the gateway host (there are no default chains):

```sh
export GUVNA_ADMIN_KEY="$ADMIN_KEY"
./guvna chains add myfree --step bazaarlink:qwen/qwen3.7-flash:free --step groq:llama-3.3-70b-versatile

# chat — any OpenAI SDK works, just point base_url at Guvna
curl http://127.0.0.1:20128/v1/chat/completions \
  -H "Authorization: Bearer $CLIENT_KEY" -H "Content-Type: application/json" \
  -d '{"model":"myfree","messages":[{"role":"user","content":"hello"}]}'
```

If `bazaarlink` is down or rate-limited, the same request transparently falls through to `groq`. Or skip chains entirely: `"model": "groq/llama-3.3-70b-versatile"`.

Manage the gateway from another terminal on the same host:

```sh
export GUVNA_ADMIN_KEY="$ADMIN_KEY"
./guvna chains list
./guvna status
./guvna status --scoring
./guvna logs -n 100
```

The API gateway must keep running. Management commands invoke the same executable
in a second process and connect to `~/.guvna/admin.sock`; combining the commands
does not imply a reduction in the gateway's memory use. Use `--socket` or
`GUVNA_ADMIN_SOCKET` when the service uses a different data directory. Public
`/admin/*` and `/v1/chains` routes have been removed, including access with an
admin key; remote `--url`/`GUVNA_URL` management is retired. See
[docs/CLI.md](docs/CLI.md) for installation, credentials and local management.
No separate TCP management port is provided.

## Configuration

Everything is YAML in git (`config.yaml`): providers, models, rotation strategy, chains. Keys live only in env (`.env`, gitignored — see `.env.example`). Per-step request params can be set inside chain steps (`params`, client values always win). See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full data model and [docs/FREE_MODELS.md](docs/FREE_MODELS.md) for per-provider free-tier limits.

Upstream response validation, body limits, cancellation and timeout settings are documented in [docs/RELIABILITY.md](docs/RELIABILITY.md).

For automatic routing, `GUVNA_DECIDER_MAX_TOKENS` controls the ranking model's output budget (default `8192`, clamped to `4096`–`32768`). NVIDIA Nemotron deciders use JSON output with thinking disabled so the budget is available for the ranking.

If a ranking refresh fails, automatic routing can reuse the last successful ranking for up to one hour, filtered to the currently eligible candidates. Logs report `source=decider_stale` while using that advice; without usable advice, routing uses the existing fallback order. Client cancellation stops retries and provider fallback without counting as a provider or key failure.

## Status

Use-ready and running in production (single user) since 2026-08-16. Compression engines and further phases are tracked in [docs/ROADMAP.md](docs/ROADMAP.md).

## Documentation

| File | What it contains |
|---|---|
| [docs/VISION.md](docs/VISION.md) | Vision, v1 goals, non-goals |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Design: components, data model, security, chains, key pools |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Deploy anywhere: Compose, binary/systemd, TLS, keys, chains |
| [docs/CLI.md](docs/CLI.md) | CLI-only management: build, credentials, status, logs and chains |
| [docs/RESEARCH.md](docs/RESEARCH.md) | Why: comparisons (new-api, litellm, OmniRoute), compression reality check |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Decision log with dates and rationale |
| [docs/FREE_MODELS.md](docs/FREE_MODELS.md) | Per-provider free model + rate-limit reference |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Build order, phase-by-phase definition of done |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: [SECURITY.md](SECURITY.md).

## License

MIT. All reference material used (OmniRoute catalog data, new-api, rtk, caveman) is MIT.
