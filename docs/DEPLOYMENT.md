# Deployment

Guvna is a single static binary with config-as-code. Two supported ways to run it.

The same `guvna` executable provides `serve`, `status`, `logs` and `chains`.
Administration uses a local filesystem socket, separate from the public model
API. No browser login or separate UI service is required or provided. See [CLI.md](CLI.md) for release downloads,
checksum verification and source builds.

## Option A — Docker Compose

```sh
git clone https://github.com/chuyua/guvna-pro && cd guvna-pro
cp .env.example .env              # add ADMIN_KEY + API_KEYS + provider keys
docker compose -f deploy/docker-compose.yml up -d
```

The compose file builds the distroless image locally (~13MB, no shell, runs as nonroot) and mounts a named volume at `/data` for SQLite telemetry + `chains.yaml`.

Or use the published multi-arch image (amd64/arm64):

```sh
docker run -d --name guvna \
  -p 127.0.0.1:20128:20128 \
  --env-file .env \
  -e GOMEMLIMIT=256MiB \
  -v guvna-data:/data \
  -v "$PWD/config.yaml:/config.yaml:ro" \
  ghcr.io/chuyua/guvna-pro:latest
```

The image contains only the `guvna` executable. Its default entrypoint preserves
the existing `-config /config.yaml -data /data` service invocation. For command
inspection without starting a service:

```sh
docker run --rm --entrypoint /guvna ghcr.io/chuyua/guvna-pro:latest --version
```

## Option B — Plain binary

Install the Linux amd64 release using the checksummed download instructions in
[CLI.md](CLI.md), or build the current checkout:

```sh
go build -o guvna ./cmd/guvna
cp .env.example .env && set -a && source .env && set +a   # or use systemd EnvironmentFile=
./guvna serve -config config.yaml                      # data dir defaults to ~/.guvna
```

Existing `guvna -config ...` and `-data ...` service commands continue to work.
A service manager can run the same command with keys supplied through its
environment. No dedicated Arch package or systemd unit is included in the release.

The model API binds `:20128` (all interfaces), or the port configured in YAML.
The bridge-mode example publishes only on host loopback; the supplied Compose
file uses host networking and needs host controls for equivalent model API
isolation. Management is never registered on this listener: `/admin/*` and all
`/v1/chains` management operations return 404, including through a reverse proxy
or Cloudflare Tunnel and even when the request has an admin key.

The management socket defaults to `<data-dir>/admin.sock` (`~/.guvna/admin.sock`
for a plain binary, `/data/admin.sock` in the image). `GUVNA_ADMIN_SOCKET` overrides
it; point the local CLI at that same path with `--socket` or the shared variable.
Linux permissions are `0600`; management also requires the admin Bearer key.
Keep the containing directory writable only by the service user; new default
data directories are created with `0700`, and existing directory modes are preserved.
There is no TCP management port or remote URL option. Do not proxy the socket or
publish it to untrusted containers. For the image, run the same executable inside
the container as its service user:

```sh
docker exec -e GUVNA_ADMIN_KEY="$ADMIN_KEY" guvna /guvna status --socket /data/admin.sock
```

A normal shutdown removes the socket. On Linux and supported Unix systems,
startup automatically replaces only a socket owned by the effective service user
whose connection attempt explicitly returns `ECONNREFUSED`. Live sockets, ordinary
files, symlinks and uncertain failures are preserved and startup fails. A
nonblocking OS lock covers inspection, cleanup and binding so concurrent starts
cannot remove a newly bound listener. Leave the persistent `<socket>.lock` marker
in place; the OS releases its lock when the process closes it or exits.

Keep custom socket paths short enough for the operating system's Unix-domain
socket path limit. Windows development requires AF_UNIX support and uses the
containing directory's ACL plus Bearer auth. Because portable socket ownership
metadata is unavailable there, existing Windows socket paths are never removed
automatically: after an unclean shutdown, confirm the original gateway is stopped
before removing only its stale `admin.sock`. A short ASCII `GUVNA_ADMIN_SOCKET`
path may be needed for a non-ASCII development profile; unsupported paths never
fall back to a TCP management listener.

## TLS exposure

Restrict access to local clients through container port publishing or host network controls unless remote clients are needed. When exposing:

- Terminate TLS in front (caddy example below). Client API authentication is always on, and keys should travel over TLS. This exposes model APIs, not local management.

```
gateway.example.com {
  reverse_proxy 127.0.0.1:20128
}
```

- If another service already binds `*:443`, put the gateway vhost on a different port (e.g. `:9443`) — two listeners on 443 cause flaky TLS handshakes.

## Keys & rotation

Provider keys live in env via `key_envs` lists in config.yaml; multiple keys per provider form a rotation pool:

- `rotation:` per provider — `round_robin` (default) / `least_used` / `sequential`
- Automatic quarantine — 401/403 → `auth` (24h), 429 → `rate_limit` (60s); cool-off doubles per consecutive failure; validated completed success resets. Service/network failures, including invalid response bodies and gateway timeouts, do not penalize keys or reset prior failures.
- Auth/rate-limit responses fail over to an unused healthy credential. Service/network setup failures retry the same healthy key up to `GUVNA_RETRIES` extra times; all extra attempts consume the shared retry budget and timed retries retain backoff. All keys down → chain falls to the next step. See [retry budgets and backoff](RELIABILITY.md#retry-budgets-and-backoff).
- Per-key state is visible with local `guvna status --json` → `keys`; telemetry rows carry the serving `key`
- Quarantine is in-memory — restart clears it

To rotate a key: edit `.env`, then `docker compose up -d --force-recreate` (or restart the process).

## Chains workflow

The gateway starts with **zero chains**. An admin creates them locally per use
case; chains persist in the data directory's `chains.yaml` and survive restarts.
Config-defined chains also load (merged, conflicts skipped with a warning).

```sh
export GUVNA_ADMIN_KEY="$ADMIN_KEY"
guvna chains list
guvna chains add myfree --step bazaarlink:qwen/qwen3.7-flash:free --step groq:llama-3.3-70b-versatile
guvna chains rm myfree
```

Chat with a chain name as the model, or bypass chains entirely with a provider-prefixed model (`groq/llama-3.3-70b-versatile`). Unknown model → 404 with a pointer to the local chain command.

If `chains.yaml` cannot be parsed, startup preserves it as a uniquely named
`chains.yaml.corrupt-<UTC timestamp>-<suffix>` backup and logs the parse error and
backup path. The gateway continues with config-defined chains. Runtime-chain
writes can then create a fresh `chains.yaml`; restore or repair records from the
backup first if you need them. If reading the file or creating the backup fails,
runtime-chain changes are rejected and the original file is left untouched. Fix
or restore the file and restart the gateway to resume chain management.

## Observability

```sh
export GUVNA_ADMIN_KEY="$ADMIN_KEY"
guvna status          # chains, step health, usage table (--json for raw)
guvna status --scoring # actual Auto outcomes and passive provider observations
guvna logs -n 100     # tail the in-memory ring
```

These commands use only the local socket. The collected metrics do not send test
calls to providers. See [AUTO-SHADOW.md](AUTO-SHADOW.md) for definitions and limits.

## Gotchas worth knowing

- Docker `--env-file` does **not** strip quotes around values (shell `source` does) — keep values unquoted
- Nonroot (UID 65532) cannot `mkdir /data` at container root — the image creates + chowns it at build time
- Client disconnects mid-stream are dropped silently (never recorded as upstream failures)

## Security defaults

- Model API auth always on; local management additionally uses its admin key and filesystem permissions
- Public listener has no admin or chain-management routes; remote CLI management is retired
- TLS via your reverse proxy when exposed; restrict the gateway port through container publishing or host network controls for local-only use
