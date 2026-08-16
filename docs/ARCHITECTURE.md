# Architecture

## High-level

```
app / any OpenAI-compatible client
        │  /v1/*  (port 20128, auth: Bearer admin/API key)
        ▼
┌──────────────────────────────────────────┐
│  BruvRoute daemon (single static binary) │
│  ┌────────────┐ ┌───────────────┐        │
│  │  Router    │ │  Compression  │        │
│  │  adaptors  │ │  pipeline     │        │
│  └────────────┘ └───────────────┘        │
│  ┌────────────┐ ┌───────────────┐        │
│  │  Catalog   │ │  Telemetry    │        │
│  │  (synced)  │ │  (SQLite)     │        │
│  └────────────┘ └───────────────┘        │
└───────────────┬──────────────────────────┘
                │ outbound HTTPS
        ┌───────┴────────┐
        │ 50+ providers  │
        └────────────────┘

Separate, optional:  read-only web UI binary  (never in core)
CLI:                bruvroute connect / status / logs (scoped tokens)
```

## Components

### 1. Router (built)
- `/v1/chat/completions` (+ `/v1/models`) OpenAI-compatible, streaming SSE passthrough, 15s keep-alives
- **Chain routing is the core primitive**: config defines named chains = ordered (provider, model) steps. Clients send chain names or any model inside a chain; unknown names hit `default_chain`. `/v1/models` exposes chain names. First 2xx step serves; non-2xx walks to the next step; all steps failed → last upstream response propagated as-is. No silent mid-stream restarts.
- **Model rewrite**: relay rewrites the request's `model` field per step (clients send logical names; upstream needs the real model). Everything else passes through untouched.
- Provider adaptors: small per-provider interface (`Chat(ctx, body) (*http.Response, error)`), pattern from new-api's `relay_adaptor.go`. Two types built: `openai` (base + `/v1/chat/completions`) and `gemini` (base + `/v1beta/openai` + chat path — Gemini's official OpenAI-compat endpoint, passthrough no translation). Outbound requests carry `User-Agent: bruvroute/0.1` (bazaarlink throttles UA-less requests).
- Auth: Bearer admin key (`ADMIN_KEY`) or client key (`API_KEYS`), always on; `/healthz` unauthenticated
- Telemetry: in-memory event buffer, async batch flush to SQLite every 30s (pure-Go driver, no CGO); hot path never touches disk; crash loses <30s
- Single process, one port. No dashboard in the process.

### 2. Compression pipeline
Decision precedence (adapted from OmniRoute): per-request header (`x-bruvroute-compression`) → named profile → adaptive → config default → off. Cache-aware compression is always on (never touch already-cached prefixes).

v1 engines:
1. **Caveman-style** — JSON rule packs + language packs + injected system prompt. Rule pack format and packs are MIT-licensed from JuliusBrussee/caveman and OmniRoute's `open-sse` compression config.
2. **RTK-style tool-output filters** — reimplement filters for ~10 dominant commands (git, grep, ls, build logs) in Go. Reference: rtk-ai/rtk (Rust CLI, no library API — reimplement, don't bind). The user runs rtk v0.43.0 locally at `~/.local/bin/rtk` for reference behavior.
3. **Session dedup** — content-addressed pruning of repeated/redundant conversation turns.

v2 (not in v1): LLMLingua-2 (ONNX MobileBERT, heavy — separate process or sidecar), headroom-style live-zone (only compress the uncached tail).

### 3. Catalog sync (subscribe-sync)
- Script pulls OmniRoute's MIT catalog data on a schedule:
  - `open-sse/config/freeModelCatalog.ts` (per-model free catalog, ~530 models)
  - `src/shared/constants/providers.ts` (per-provider `hasFree` + `freeNote`)
- Diff + commit to git. We own the sync layer, not the weekly re-audit.
- Data is declarative and MIT — legally liftable. Data persists in git history even if upstream dies.

### 4. Config & data
- **Config-as-code**: YAML in git (`config.yaml`): providers, keys via env, profiles, compression settings, ports, routing weights
- **SQLite**: usage/telemetry only (request counts, tokens, errors). Never for config.
- Data dir: `~/.bruvroute/` (config override, db, logs)

### 5. Security
- Auth always on: one admin key (env `ADMIN_KEY` / `JWT_SECRET`), API keys for clients
- Scoped tokens for remote CLI (`bruvroute connect`), revocable
- No multi-user, no registration, no open dashboard

### 6. Web UI (v1, minimal, separate)
- Read-only status: provider health, usage, token savings
- Separate binary; can be omitted from deployment entirely

## Sizing

- Core gateway: ~3-6k lines of Go (scoped reimplementation of OmniRoute's routing subset)
- RSS: ~15-40MB idle
- Distroless image ~10-20MB
- Rationale in RESEARCH.md (why scratch, not port or fork)

## Non-negotiable defaults

- `REQUIRE_API_KEY` equivalent always on when exposed
- Config in git, not DB
- UI never in core process
- Port 20128 (app drop-in)
