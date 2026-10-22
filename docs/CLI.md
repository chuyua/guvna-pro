# CLI management

One `guvna` executable starts the gateway with `serve` and manages it with
`status`, `logs` and `chains`. Management commands run on the gateway host and
communicate through a private filesystem socket. OpenAI-compatible applications
use the separate model API, normally on port 20128. There is no browser login,
OAuth flow or additional TCP management port.

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

Leave the gateway running. In a second terminal on the same host, set its admin credential. If that terminal has already loaded the gateway environment:

```sh
export GUVNA_ADMIN_KEY="$ADMIN_KEY"
```

```powershell
$env:GUVNA_ADMIN_KEY = $env:ADMIN_KEY
```

Otherwise set `GUVNA_ADMIN_KEY` to the same private value as the gateway's `ADMIN_KEY`. No browser sign-in or external login account is involved.

Management is local-only. The CLI connects to the filesystem socket
`~/.guvna/admin.sock`; it has no TCP or remote URL fallback. The service uses
`<data-dir>/admin.sock`, so a service started with `-data /data` needs CLI
`--socket /data/admin.sock`. `GUVNA_ADMIN_SOCKET` can set the same socket path for
both processes. On Linux the socket is mode `0600`; the CLI must run as the
service user and still supply the admin key. Windows development uses the parent
directory's ACL and the same Bearer authentication; use `GUVNA_ADMIN_SOCKET`
with a short ASCII path if the development profile's non-ASCII path is unsupported.
Unsupported Unix sockets or
overlong paths fail explicitly rather than enabling network management.

The public gateway listener continues serving model APIs and `/healthz`, but
returns 404 for `/admin/*` and `/v1/chains`, even with the admin key or a
loopback/forwarded peer address. `--url` and `GUVNA_URL` remote management are
retired. Run the CLI on the gateway host (or in its container); see
[DEPLOYMENT.md](DEPLOYMENT.md).

## Status and logs

```sh
./guvna status
./guvna status --json
./guvna status --scoring
./guvna status --scoring --since 2026-10-21T00:00:00Z --until 2026-10-21T01:00:00Z --json
./guvna logs -n 100
./guvna logs -n 100 --json
```

Status shows telemetry queue/in-flight lengths, dropped events, successful and
failed flush counts, and retained events awaiting retry. If persisted usage
cannot be queried, status still shows these in-memory diagnostics and reports
`usage_error`. `status --json` preserves the complete server response,
including key state and fields introduced by newer gateways.

`status --scoring` shows actual Auto request results, provider failure classes
and current model cooldowns through the local socket; `--json` also includes the
latest shadow comparisons. Normal Auto traffic is collected in every mode; enabling shadow is not
required for collection. See [AUTO-SHADOW.md](AUTO-SHADOW.md) for metric meanings,
time filters and activation requirements. The default route continues to use the
decider; these queries do not probe providers.

## Runtime chains

```sh
./guvna chains list
./guvna chains list --json
./guvna chains add myfree --step bazaarlink:qwen/qwen3.7-flash:free --step groq:llama-3.3-70b-versatile
./guvna chains rm myfree
```

Each step is `provider:model`; provider names must exist in the gateway configuration. Runtime chains are persisted in the gateway's data directory. The CLI does not currently express per-step `params`; define them in configuration or use the authenticated management protocol over the local socket when needed. Client API keys cannot manage chains.

`chains list` includes inactive file records with `SOURCE` set to `dormant`;
`--json` also includes their original steps and per-step `params`. Step params
that JSON cannot express (such as YAML mappings with non-string keys or `.nan`)
are returned as a list-only `params_yaml` string instead of `params`. These records
are retained when a provider is unavailable or a config-defined chain has the
same name. Config takes precedence for routing, so a shared name appears twice:
once as `config` and once as `dormant`. Dormant records do not execute. They are
retried on the next startup/load, and become active if their providers are
available, their definitions are valid, and no config chain shadows them.

`chains rm NAME` removes a runtime chain or its dormant file record. If the name
has both a config chain and a dormant record, only the dormant record is deleted;
the config chain keeps routing. Removing a config-defined chain without a dormant
record is refused: edit its YAML configuration instead. A failed persistence
leaves both the record and the active routes unchanged.

`--socket PATH` and `--token KEY` override the environment for an individual command. Prefer the environment for the token so it is not written into shell history. `--json` emits raw JSON for status, logs, chain lists and chain creation. For example:

```sh
./guvna status --socket /data/admin.sock --json
./guvna chains add myfree --step groq:llama-3.3-70b-versatile --json
```

Run `./guvna help` for the command summary. There is no `login` or `connect`
subcommand. A single executable simplifies installation and updates; it does not
promise a significant reduction in the gateway's memory use.
