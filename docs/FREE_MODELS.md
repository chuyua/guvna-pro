# Free Models & Rate Limits

Reference for integrations: which models are free on each provider, and the free-tier rate limits. All limits are free-tier (no credit card). Limits drift — verify before relying on them. Last checked: 2026-08-17.

Legend: RPM = requests/min, TPM = tokens/min, RPD = requests/day, TPD = tokens/day. `0/0/0` = no free tier.

## Google (AI Studio)

Per-project limits.

| Model (config id) | RPM | TPM | RPD |
|---|---|---|---|
| gemini-2.5-flash | 5 | 250K | 20 |
| gemini-3-flash-preview | 5 | 250K | 20 |
| gemma-4-31b-it | 30 | 16K | 14.4K |
| gemma-4-26b-a4b-it | 30 | 16K | 14.4K |
| Gemini 2.5 Flash Lite | 10 | 250K | 20 |
| Gemini 3.1 Flash Lite | 15 | 250K | 500 |
| Gemini 3.5 Flash | 5 | 250K | 20 |
| Gemini 3.5 Flash Lite | 15 | 250K | 500 |
| Gemini 3.6 Flash | 5 | 250K | 20 |
| Gemini 3.7 Flash | 5 | 250K | 20 |
| Gemini Embedding 1 / 2 | 100 | 30K | 1K |
| Antigravity (agents) | 60 | 100K | 100 |

No free tier (0/0/0): Gemini 2 Flash, 2.5 Pro, 3.1 Pro, Nano Banana family, Gemini Omni Flash, Computer Use, Deep Research Pro, Veo 3, Lyria 3.

## Groq

Per-organization limits. NOTE: groq keys in gateway were 403 (revoked) as of 2026-08-17.

| Model | RPM | RPD | TPM | TPD |
|---|---|---|---|---|
| llama-3.3-70b-versatile | 30 | 1K | 12K | 100K |
| gpt-oss-120b / openai/gpt-oss-120b | 30 | 1K | 8K | 200K |
| openai/gpt-oss-20b | 30 | 1K | 8K | 200K |
| qwen/qwen3.6-27b | 30 | 1K | 8K | 200K |
| llama-3.1-8b-instant | 30 | 14.4K | 6K | 500K |
| groq/compound | 30 | 250 | 70K | — |

## Mistral (La Plateforme, Experiment tier)

No credit card (SMS verify). Global **1 req/sec per API key** across all models; ~1B tokens/month cap. Per-pool quotas: standard pool (small, large-2512, codestral, ministral-*) 50K tokens/min + 4M tokens/month; mistral-medium-2508 375K tok/min / 25 req/min; mistral-large-2411 600K tok/5min / 60 req/min.

## Bazaarlink

No credit card. Account-wide (not per-model): **10 RPM / 50 RPD** (×1). After any top-up: ×2 = 20 RPM / 100 RPD. Past quota on free models (`:free`) continues at paid rate if credit exists.

Free models in config: `qwen/qwen3.7-flash:free`, `deepseek/deepseek-v4-flash:free`, plus `auto:free` router alias.

## OrcaRouter (Hacker tier)

Full gateway free; no published per-model free limits. After wallet top-up, usage bills at upstream provider rate (zero markup). Free-tier models in config: `orcarouter/free`, `deepseek/deepseek-v4-flash-free`. Catalog mirrors OpenRouter's namespaced model set, so `:free`-style variants likely inherit OpenRouter-style limits (20 RPM / 50 RPD class) — verify via `X-RateLimit-*` headers.

## Chain design implications

Gemini free RPD is tiny (20/day for flash) — keep gemini as last-resort in chains, groq/mistral as primary. Bazaarlink/orcarouter per-account caps mean shared keys throttle across all users.