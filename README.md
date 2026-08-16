# BruvRoute

Low-resource, single-user AI gateway in Go. Makes free-tier quotas go 2-3x further via token compression. Cloud-first by design.

## What

- One `/v1` OpenAI-compatible endpoint (port 20128) routing to 50+ free-tier providers
- Token compression = quota stretching, not bill savings (free tiers bill in tokens)
- Headless daemon, single static binary, config-as-code YAML in git
- ~15-40MB RSS; distroless image ~10-20MB; one-command deploy
- Secure by default: auth always on, scoped remote CLI tokens

## Status

Pre-build. Docs only. The docs below are the full handoff — a fresh session can pick up and start building from them.

## Docs index

| File | What it contains |
|---|---|
| [docs/VISION.md](docs/VISION.md) | Vision, v1 goals, non-goals |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Design: components, data model, security, compression |
| [docs/RESEARCH.md](docs/RESEARCH.md) | Why: comparisons (new-api, litellm, OmniRoute), compression reality check, reference sources |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Build order, phase-by-phase definition of done (no timings) |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Target environments: Arch + the VPS VPS, app drop-in facts |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Decision log with dates and rationale |

## License

MIT. All reference material used (OmniRoute catalog data, new-api, rtk, caveman) is MIT.
