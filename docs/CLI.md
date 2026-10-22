# CLI management

One `guvna` executable starts the gateway with `serve` and manages it with
`status`, `logs` and `chains`. Management commands run in a second process and
talk to the running gateway's HTTP API, normally on port 20128. There is no
browser login, OAuth flow or separate UI service on port 20129. OpenAI-compatible
applications use that same API for inference.

## Install the Linux amd64 release

The only standalone release target is Linux amd64. Download the executable and
its checksum manifest into the same directory:

```sh
curl -fLO https://github.com/chuyua/guvna-pro/releases/latest/download/guvna
curl -fLO https://github.com/chuyua/guvna-pro/releases/latest/download/SHA256SUMS
sha256sum --check SHA256SUMS
chmod +x guvna
./guvna --version
```

Use `releases/download/<tag>/...` instead of `releases/latest/download/...` to
pin a release. The version output is `guvna <tag>`. Release assets do not include
a separate `guvna-cli`, Windows executable or Arch package. The existing Docker
image remains available for amd64/arm64 and contains the same command surface.

The executable download does not include configuration or credentials. Obtain
`config.yaml` and `.env.example` from the matching tag's source archive, create
your private `.env`, and follow the startup section below.

## Build from the current checkout

Run these commands at the repository root on Linux or macOS:

```sh
go build -o guvna ./cmd/guvna
```

PowerShell on Windows:

```powershell
go build -o guvna.exe ./cmd/guvna
```

These are source builds, not additional release targets. They report `guvna dev`
by default. A versioned build uses, for example,
`go build -ldflags="-X main.version=v1.0.0" -o guvna ./cmd/guvna`.

The examples below use `./guvna`. In PowerShell use `.\guvna.exe` with the same
arguments. `./guvna --help` and `./guvna --version` do not require a config or
credentials and do not start the gateway.

## Credentials and startup

The gateway requires `ADMIN_KEY`, one or more comma-separated `API_KEYS`, and the keys referenced by the configured providers. Set these through your environment or service manager; `.env.example` lists the settings. Keep actual values out of version control.

- `ADMIN_KEY` is the gateway's own administrative Bearer credential. The CLI reads the same value through `GUVNA_ADMIN_KEY`.
- `API_KEYS` are the gateway's client Bearer credentials for applications. They are separate from upstream provider keys.
- Provider keys authenticate the gateway's outbound requests to the providers configured in `config.yaml`.

After creating a private `.env` from `.env.example`, a POSIX shell can start the gateway with:

```sh
set -a; source .env; set +a
./guvna serve -config config.yaml
```

In PowerShell, set the required environment variables in the process or load them through your service manager, then start the gateway:

```powershell
$env:ADMIN_KEY = '<your-gateway-admin-key>'
$env:API_KEYS = '<your-gateway-client-key>'
# Also set the provider key environment variables named in config.yaml.
.\guvna.exe serve -config config.yaml
```

Existing service invocations without `serve`, including `guvna -config ...`
and `guvna -data ...`, remain supported. Running `guvna` with no arguments also
starts the gateway with its existing defaults. `guvna --healthcheck -config ...`
still probes the running gateway's `/healthz`; it does not start a service or
call a model. Use `guvna serve --help` for service flags.

Leave the gateway running. In a second terminal, set the CLI destination and its admin credential. If that terminal has already loaded the gateway environment:

```sh
export GUVNA_URL=http://127.0.0.1:20128
export GUVNA_ADMIN_KEY="$ADMIN_KEY"
```

```powershell
$env:GUVNA_URL = 'http://127.0.0.1:20128'
$env:GUVNA_ADMIN_KEY = $env:ADMIN_KEY
```

Otherwise set `GUVNA_ADMIN_KEY` to the same private value as the gateway's `ADMIN_KEY`. No browser sign-in or external login account is involved. For a remote gateway, use its HTTPS URL and the matching admin key.

The CLI's loopback default is a destination, not a server bind restriction. The gateway currently listens on `:port` (all interfaces). Configure container publishing or host network controls for local-only access, and HTTPS plus network access controls for remote access. See [DEPLOYMENT.md](DEPLOYMENT.md).

## Status and logs

```sh
./guvna status
./guvna status --json
./guvna logs -n 100
./guvna logs -n 100 --json
```

Status shows telemetry queue/in-flight lengths, dropped events, successful and
failed flush counts, and retained events awaiting retry. If persisted usage
cannot be queried, status still shows these in-memory diagnostics and reports
`usage_error`. `status --json` preserves the complete server response,
including key state and fields introduced by newer gateways.

Auto ordering experiment metrics and the latest bounded comparison records are
available through the authenticated `GET /admin/auto/scoring` API. See
[AUTO-SHADOW.md](AUTO-SHADOW.md) for time filters, definitions and activation
requirements. The default route continues to use the decider.

## Runtime chains

```sh
./guvna chains list
./guvna chains list --json
./guvna chains add myfree --step bazaarlink:qwen/qwen3.7-flash:free --step groq:llama-3.3-70b-versatile
./guvna chains rm myfree
```

Each step is `provider:model`; provider names must exist in the gateway configuration. Runtime chains are persisted in the gateway's data directory. Removing a config-defined chain is refused: edit its YAML configuration instead. The CLI does not currently express per-step `params`; use the JSON API when those are needed.

`--url URL` and `--token KEY` override the environment for an individual command. Prefer the environment for the token so it is not written into shell history. `--json` emits raw JSON for status, logs, chain lists and chain creation. For example:

```sh
./guvna status --url https://your-gateway.example.com --json
./guvna chains add myfree --step groq:llama-3.3-70b-versatile --json
```

Run `./guvna help` for the command summary. There is no `login` or `connect`
subcommand. A single executable simplifies installation and updates; it does not
promise a significant reduction in the gateway's memory use.
