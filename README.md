# DataBridge

DataBridge is a Python HTTP service that stores local and SFTP connections,
previews CSV/JSON files, and copies files between connections without changing
their bytes. It implements all three phases of the [take-home brief](instructions/README.md),
plus encrypted SFTP credentials, bounded-memory transfer, healthcheck, connector
contract tests, and served OpenAPI documentation.

## Quick start

Install Python 3.12, [uv](https://docs.astral.sh/uv/), and Docker Compose. From
the repository root, copy this single command:

```bash
make quickstart
```

This installs dependencies, creates `.env` only if it is missing, starts the
SFTP fixture, captures its local host key only if no trusted key is saved, runs
the checks, and starts the API. It takes longer on the first run because it
downloads dependencies and the SFTP image. Stop the API with Ctrl-C. Later,
`make run` starts only the API.

Retain `.env` across restarts. The key is required at startup and is never
stored in SQLite. `make run` serves `http://127.0.0.1:8080` with one worker.
In another terminal, `make logs` follows the request log at
`state/databridge.requests.jsonl`. Each JSON line includes a request ID,
status, route, duration, and allowlisted request parameters. The SFTP password
field, encryption key, and raw request body are not logged. The response's
`X-Request-ID` header identifies its log entry; the log file is ignored by Git.
The database defaults to `state/databridge.sqlite3`. `make smoke` starts its
own temporary API process on a random localhost port; it does not need
`make run` to be active. It verifies
local and remote preview, transfers both ways, saved status, and matching hashes
against Docker SFTP.

The first run explicitly trusts the key presented by this **local test
container**. Later runs retain the saved key; runtime SFTP connections reject
untrusted or changed keys. The Compose project stores server host keys in a
named volume, so normal container recreation keeps them stable.
`docker compose down -v` removes that volume. If you intentionally remove it,
run `rm known_hosts` before the next `make quickstart` to trust the replacement
local container key. The image is linux/amd64; on ARM
Docker engines it runs under emulation. If the SFTP mount is not writable on
your platform, inspect permissions on `sftp_data/` before running integration.

## API examples

The running service provides [Swagger UI](http://127.0.0.1:8080/docs) and
[OpenAPI JSON](http://127.0.0.1:8080/openapi.json). These commands use the
supplied `data/customers.csv` and `data/products.json` fixtures. Run them from
the repository root after `make run` is listening:

For `POST /connections` in Swagger UI, select the `local_data`, `local_output`,
or `sftp` example before sending it. The generic `"string"` placeholder is a
literal path, not a reference to the sample data.
For `POST /transfers`, select the `upload` or `download` example. Create the
named connections first, then run the upload before the download. The transfer
uses the filename at each connection's root, and a repeated destination returns
409 unless you explicitly set `"overwrite":true`.

```bash
curl -sS -X POST http://127.0.0.1:8080/connections \
  -H 'Content-Type: application/json' \
  -d '{"name":"local_data","type":"local","path":"data"}'

curl -sS -X POST http://127.0.0.1:8080/connections \
  -H 'Content-Type: application/json' \
  -d '{"name":"local_output","type":"local","path":"output"}'

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
root access. Responses never include the stored password. An existing local
connection directory is reused; a missing one is created when the
connection is created. Relative local paths such as `data` and `output` are
resolved from the running service's working directory (the repository root for
`make run` and `make quickstart`). An existing destination is refused unless
`"overwrite":true` is passed. The transfer
response includes status, UTC timestamps, bytes copied, and an ID usable at
`GET /transfers/{id}`. Failures use a stable error code and include that ID if
a transfer record was created.

File names are limited to one file at the configured root. Listings return at
most 1,000 names after scanning at most 10,000 entries, with `truncated` when a
cap stops enumeration. Preview returns five rows by default (maximum 100),
parses at most 1 MiB, and reads one extra byte to detect truncation. CSV values
remain strings; JSON values retain their types.
Schema inference ignores empty/null values, widens integer plus float to float,
and uses string for incompatible values or an entirely empty field. JSON numeric
and boolean strings remain strings; `YYYY-MM-DD` strings that name a real day
infer as dates, and numbers use ASCII digits only. A small JSON
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

Writes stage under `.databridge-{transfer-id}.part` and publish
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
