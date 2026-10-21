# CLI management

Guvna is managed with `guvna-cli`; there is no browser login, OAuth flow or separate UI service on port 20129. The CLI talks to the running `guvna` HTTP API, normally on port 20128. OpenAI-compatible applications use that same API for inference.

## Build from the current checkout

Run these commands at the repository root on Linux or macOS:

```sh
go build -o guvna ./cmd/guvna
go build -o guvna-cli ./cmd/guvna-cli
```

PowerShell on Windows:

```powershell
go build -o guvna.exe ./cmd/guvna
go build -o guvna-cli.exe ./cmd/guvna-cli
```

The examples below use `./guvna-cli`. In PowerShell use `.\guvna-cli.exe` with the same arguments.

## Credentials and startup

The gateway requires `ADMIN_KEY`, one or more comma-separated `API_KEYS`, and the keys referenced by the configured providers. Set these through your environment or service manager; `.env.example` lists the settings. Keep actual values out of version control.

- `ADMIN_KEY` is the gateway's own administrative Bearer credential. The CLI reads the same value through `GUVNA_ADMIN_KEY`.
- `API_KEYS` are the gateway's client Bearer credentials for applications. They are separate from upstream provider keys.
- Provider keys authenticate the gateway's outbound requests to the providers configured in `config.yaml`.

After creating a private `.env` from `.env.example`, a POSIX shell can start the gateway with:

```sh
set -a; source .env; set +a
./guvna -config config.yaml
```

In PowerShell, set the required environment variables in the process or load them through your service manager, then start the gateway:

```powershell
$env:ADMIN_KEY = '<your-gateway-admin-key>'
$env:API_KEYS = '<your-gateway-client-key>'
# Also set the provider key environment variables named in config.yaml.
.\guvna.exe -config config.yaml
```

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
./guvna-cli status
./guvna-cli status --json
./guvna-cli logs -n 100
./guvna-cli logs -n 100 --json
```

## Runtime chains

```sh
./guvna-cli chains list
./guvna-cli chains list --json
./guvna-cli chains add myfree --step bazaarlink:qwen/qwen3.7-flash:free --step groq:llama-3.3-70b-versatile
./guvna-cli chains rm myfree
```

Each step is `provider:model`; provider names must exist in the gateway configuration. Runtime chains are persisted in the gateway's data directory. Removing a config-defined chain is refused: edit its YAML configuration instead. The CLI does not currently express per-step `params`; use the JSON API when those are needed.

`--url URL` and `--token KEY` override the environment for an individual command. Prefer the environment for the token so it is not written into shell history. `--json` emits raw JSON for status, logs, chain lists and chain creation. For example:

```sh
./guvna-cli status --url https://your-gateway.example.com --json
./guvna-cli chains add myfree --step groq:llama-3.3-70b-versatile --json
```

Run `./guvna-cli help` for the command summary. There is no `login` or `connect` subcommand in the current CLI.
