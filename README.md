# BruvRoute

Low-resource, single-user AI gateway in Go. Cloud-first by design, use-ready and running in production (single-user production) since 2026-08-16.

## What

- One `/v1` OpenAI-compatible endpoint (port 20128) routing to free-tier providers: Bazaarlink, Groq, Mistral, Gemini, OrcaRouter
- **Chain-based routing**: clients create named chains (ordered provider/model fallback steps) per use case — no default chains, no real provider model names in clients
- Provider-prefixed passthrough: call any catalog model directly as `<provider-prefix>/<model>`
- **Key pools**: per-provider rotation (round-robin / least-used / sequential) with class-based quarantine (auth/rate-limit/transient) and automatic key failover — one dead key never burns the pool
- Failure marking: unhealthy providers get cool-off (60s → 10min, auto-recovery), chains fail over across steps, no silent mid-stream restarts
- Headless daemon, single static binary (~16MB), distroless image ~13MB, ~15-40MB RSS, config-as-code YAML in git
- Secure by default: auth always on (admin + client API keys), remote CLI over the gateway's HTTPS endpoint
- Observability: async SQLite telemetry (requests, tokens, serving key), `/admin/status` + `/admin/logs`, `bruvroute-cli status|logs|chains`

## Status

Use-ready. Live on the the VPS VPS at `https://gateway.example.com:9443/v1` (caddy → distroless container, 400m cap, GOMEMLIMIT 256MiB). Custom chains API, key rotation/quarantine, and remote CLI are built and verified live. Compression engines and the rest of the roadmap are tracked in [docs/ROADMAP.md](docs/ROADMAP.md).

## Docs index

| File | What it contains |
|---|---|
| [docs/VISION.md](docs/VISION.md) | Vision, v1 goals, non-goals |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Design: components, data model, security, chains, key pools |
| [docs/RESEARCH.md](docs/RESEARCH.md) | Why: comparisons (new-api, litellm, OmniRoute), compression reality check, reference sources |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Build order, phase-by-phase definition of done (no timings) |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Deploy: the VPS (live), Arch (deferred), chains + keys workflow |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Decision log with dates and rationale |

## License

MIT. All reference material used (OmniRoute catalog data, new-api, rtk, caveman) is MIT.
