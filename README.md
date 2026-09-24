# DataBridge

DataBridge is a Python HTTP service that stores local and SFTP connections,
previews CSV/JSON files, and copies files between connections without changing
their bytes. It implements all three phases of the [take-home brief](instructions/README.md),
plus encrypted SFTP credentials, bounded-memory transfer, healthcheck, connector
contract tests, and served OpenAPI documentation.

## Quick start

Use Python 3.12, [uv](https://docs.astral.sh/uv/), and Docker Compose. From the
repository root:

```bash
uv sync --extra dev
if [ ! -f .env ]; then
  uv run python -c 'from cryptography.fernet import Fernet; print("DATABRIDGE_ENCRYPTION_KEY=" + Fernet.generate_key().decode())' > .env
fi
chmod 600 .env
mkdir -p sftp_data
docker compose up -d
ssh-keyscan -T 5 -p 2222 127.0.0.1 > known_hosts
chmod 600 known_hosts
make check
make integration
make smoke
make run
```

Create `.env` **once** and retain it across restarts. The key is required at
startup and is never stored in SQLite. `make run` serves
`http://127.0.0.1:8080` with one worker. The database defaults to
`state/databridge.sqlite3`. `make smoke` starts its own temporary API process
on a random localhost port; it does not need `make run` to be active. It verifies
local and remote preview, transfers both ways, saved status, and matching hashes
against Docker SFTP.

The `ssh-keyscan` step explicitly trusts the key presented by this **local test
container**. Runtime SFTP connections reject untrusted or changed keys. The
Compose project stores server host keys in a named volume, so normal container
recreation keeps them stable. `docker compose down -v` removes that volume;
repeat the trust bootstrap if you use it. The image is linux/amd64; on ARM
Docker engines it runs under emulation. If the SFTP mount is not writable on
your platform, inspect permissions on `sftp_data/` before running integration.

## API examples

The running service provides [Swagger UI](http://127.0.0.1:8080/docs) and
[OpenAPI JSON](http://127.0.0.1:8080/openapi.json). These commands use the
supplied `data/customers.csv` and `data/products.json` fixtures. Run them from
the repository root after `make run` is listening:

```bash
mkdir -p output
DATA_ROOT="$(pwd)/data"
OUTPUT_ROOT="$(pwd)/output"

curl -sS -X POST http://127.0.0.1:8080/connections \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"local_data\",\"type\":\"local\",\"path\":\"$DATA_ROOT\"}"

curl -sS -X POST http://127.0.0.1:8080/connections \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"local_output\",\"type\":\"local\",\"path\":\"$OUTPUT_ROOT\"}"

curl -sS -X POST http://127.0.0.1:8080/connections \
  -H 'Content-Type: application/json' \
  -d '{"name":"remote_server","type":"sftp","host":"127.0.0.1","port":2222,"username":"testuser","password":"testpass","root":"data"}'

curl -sS http://127.0.0.1:8080/connections
curl -sS http://127.0.0.1:8080/connections/local_data/files
curl -sS 'http://127.0.0.1:8080/connections/local_data/files/customers.csv/head?limit=5'
curl -sS -X POST http://127.0.0.1:8080/connections/remote_server/healthcheck

curl -sS -X POST http://127.0.0.1:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{"source":"local_data","source_file":"customers.csv","destination":"remote_server","destination_file":"customers.csv"}'

curl -sS -X POST http://127.0.0.1:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{"source":"remote_server","source_file":"customers.csv","destination":"local_output","destination_file":"downloaded.csv"}'

# Paste an ID returned by POST /transfers:
curl -sS http://127.0.0.1:8080/transfers/TRANSFER_ID
shasum -a 256 data/customers.csv sftp_data/customers.csv output/downloaded.csv
```

Connections persist across service restarts. SFTP connection creation checks
the request shape; healthcheck or file operations verify live credentials and
root access. Responses never include the stored password. An existing
destination is refused unless `"overwrite":true` is passed. The transfer
response includes status, UTC timestamps, bytes copied, and an ID usable at
`GET /transfers/{id}`. Failures use a stable error code and include that ID if
a transfer record was created.

File names are limited to one file at the configured root. Listings return at
most 1,000 names after scanning at most 10,000 entries, with `truncated` when a
cap stops enumeration. Preview returns five rows by default (maximum 100) and
reads at most 1 MiB. CSV values remain strings; JSON values retain their types.
Schema inference ignores empty/null values, widens integer plus float to float,
and uses string for incompatible values or an entirely empty field. JSON numeric
and boolean strings remain strings; ISO date strings infer as dates. A small JSON
file is validated in full; for larger files, the preview validates the bounded
prefix it reads. Transfer copies 1 MiB binary chunks and does not parse files.

## Design and verification

The [connector protocol](databridge/connectors/base.py) defines list, read, and
write. [Local](databridge/connectors/local.py) and
[SFTP](databridge/connectors/sftp.py) connectors own file opening, staging,
publication, and cleanup. The [transfer service](databridge/transfer.py) knows
only that protocol; a third test connector exercises the same copy logic. The
[store](databridge/store.py) keeps connection settings and transfer records in
SQLite and encrypts SFTP passwords with a stable external Fernet key. The API
uses typed request/response models and maps safe domain errors to HTTP status.

Writes stage under `.{destination}.databridge-{transfer-id}.part` and publish
only on success. A handled failure removes its stage and leaves the existing
destination unchanged. After a process crash, startup marks a `running` record
`failed`, but an unpublished stage may remain. Inspect the failed transfer ID
before manually removing its matching `.part` file from the configured root.
The one-process database rule is intentional: another worker could otherwise
mark a live transfer as interrupted.

`make check` runs Ruff and local/API tests. `make integration` runs the shared
connector contract and Docker SFTP tests. `make smoke` is a live HTTP reviewer
path. `make benchmark` checks SHA-256 and measures 16 MiB and 256 MiB transfers
both ways. The [measured results](docs/benchmark.md) include throughput, peak
process memory, host architecture, emulation, and an explicitly untested 1 GiB
estimate. The [requirement evidence map](verification.md) ties R1–R14 to exact
checks. [Intent](intent.md), [specification](spec.md), and the
[AI collaboration log](docs/ai-work-log.md) show the decisions and corrections
behind the code. The required [AI collaboration summary](ai-collaboration-summary.md)
describes who directed and produced the work.

## Limits and next steps

This is a localhost, trusted-development API with one shared namespace and no
caller authentication or tenant isolation. Multiple callers can use it, but all
have the same access. A shared deployment would need authenticated tenant
identity, tenant-scoped storage and file roots, authorization, and network
controls for user-supplied SFTP hosts. An instance per tenant is a simpler
alternative. Transfers are synchronous, assume the source remains stable, and
do not resume after interruption. Staging and collision behavior were tested
against the supplied SFTP server; other servers may differ. Key rotation,
background jobs, more SFTP servers, and concurrent-load testing are reasonable
follow-ups. No extra connector type or idempotency layer was added.
