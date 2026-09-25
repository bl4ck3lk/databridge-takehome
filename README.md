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

This installs dependencies, adds missing settings to `.env` (it never replaces
one), starts the SFTP fixture, trusts its host key if none is saved, runs the
checks, and starts the API. It takes longer on the first run because it
downloads dependencies and the SFTP image. Stop the API with Ctrl-C. Later,
`make run` starts only the API.

| `.env` setting | Written by quickstart as |
| --- | --- |
| `DATABRIDGE_ENCRYPTION_KEY` | A new Fernet key; required at startup, never stored in SQLite |
| `DATABRIDGE_DB_PATH`, `DATABRIDGE_KNOWN_HOSTS` | Absolute paths to `state/databridge.sqlite3` and `known_hosts` |
| `DATABRIDGE_SFTP_PORT` | `2222`, the fixture's host port |
| `DATABRIDGE_SFTP_UID` | Your UID, so the fixture can write the bind-mounted `sftp_data/` on Linux |
| `COMPOSE_PROJECT_NAME` | A name derived from the checkout path, so two checkouts never share a fixture |

`DATABRIDGE_ALLOWED_HOSTS` (comma-separated) replaces the accepted `Host` names
`127.0.0.1`, `localhost`, and `::1`.

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

### Trusting the SFTP fixture

The first run trusts the key presented by this **local test container**: it
scans `127.0.0.1` once and trusts `localhost` with the same keys, so both
names work. Later runs keep the saved keys, and every SFTP connection is refused
before authentication when a key is unknown or changed; the error names the
`[host]:port` entry, the key's SHA-256 fingerprint, the trust file, and the
command that fixes it. The Compose project keeps the server's host keys in a
named volume, so normal container recreation keeps them stable.

The keys change when that volume is removed (`docker compose down -v`) or when
the brief's own `instructions/docker-compose.yml`, which has no key volume,
recreates the container. After verifying that you intend to trust the new
container, remove the old entries and rerun quickstart:

```bash
ssh-keygen -R '[127.0.0.1]:2222' -f known_hosts
ssh-keygen -R '[localhost]:2222' -f known_hosts
make quickstart
```

The image is linux/amd64; on ARM Docker engines it runs under emulation.

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
# {"connection":"remote_server","reachable":true,"writable":true}
curl -sS -X POST http://127.0.0.1:8080/connections/remote_server/healthcheck

curl -sS -X POST http://127.0.0.1:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{"source":"local_data","source_file":"customers.csv","destination":"remote_server","destination_file":"customers.csv"}'

curl -sS -X POST http://127.0.0.1:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{"source":"remote_server","source_file":"customers.csv","destination":"local_output","destination_file":"downloaded.csv"}'

# Paste an ID returned by POST /transfers, or list the newest transfers:
curl -sS http://127.0.0.1:8080/transfers/TRANSFER_ID
curl -sS 'http://127.0.0.1:8080/transfers?status=running&limit=10'
shasum -a 256 data/customers.csv sftp_data/customers.csv output/downloaded.csv

# Replace every setting of a connection (send the SFTP password again), or delete it:
curl -sS -X PUT http://127.0.0.1:8080/connections/local_output \
  -H 'Content-Type: application/json' \
  -d '{"name":"local_output","type":"local","path":"output2"}'
curl -sS -X DELETE http://127.0.0.1:8080/connections/local_output
```

Connections persist across service restarts. SFTP connection creation checks
the request shape; the healthcheck and file operations verify live credentials
and root access, and the healthcheck also reports whether a probe file could be
written. Responses never include the stored password. An existing local
connection directory is reused; a missing one is created when the
connection is created. Relative local paths such as `data` and `output` are
resolved from the running service's working directory (the repository root for
`make run` and `make quickstart`). Deleting a connection keeps its files and its
transfer records. An existing destination is refused unless `"overwrite":true`
is passed. A transfer record has a status (`running`, `publishing`,
`completed`, or `failed`), UTC timestamps, `bytes_copied` (saved about once per
second while copying, so `GET /transfers?status=running` shows progress), and,
after a failure, the failed phase, its error code, and a message that names the
failing side. Failures include the transfer ID when a record was created. At most
four transfers copy at once; another request gets `TRANSFER_CAPACITY_EXCEEDED`.
Every error code and its HTTP status are listed in the [specification](spec.md).

### Differences from the brief

| Brief | DataBridge |
| --- | --- |
| SFTP body uses `user` and has no root | `user` is accepted for `username`; `root` is required (the fixture uses `"root":"data"`), so a transfer never guesses where files live |
| Host `localhost` | Works after `make quickstart`, which trusts the fixture under `127.0.0.1` and `localhost`; host names are stored in lower case |
| Unknown request fields | Rejected with the field named |
| Transfer onto an existing file | Refused unless `"overwrite":true` |
| Compose file in `instructions/` | The root `docker-compose.yml` adds a host-key volume, a configurable port and UID, and a per-checkout project name |

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

The [connector protocol](databridge/connectors/base.py) defines list, access
check, read, and write, and its error contract: every failure is a
`DataBridgeError` with a typed code. [Local](databridge/connectors/local.py) and
[SFTP](databridge/connectors/sftp.py) connectors own root preparation, file
opening, staging, publication, and cleanup; a
[registry](databridge/connectors/__init__.py) maps each connection type to its
connector and fails closed on an unknown type. The
[transfer service](databridge/transfer.py) knows only that protocol and a
record-keeping protocol; a third test connector exercises the same copy logic.
The [store](databridge/store.py) keeps connection settings and transfer records
in SQLite and encrypts SFTP passwords with a stable external Fernet key. The API
validates requests and maps error codes to HTTP statuses.

The SFTP connector checks the reply to every request, keeps up to 64 requests
of 32 KiB in flight per file, requests `fsync@openssh.com` where the server
supports it, and verifies the staged size before publishing. A watchdog closes a
session when one request or reply takes more than 30 seconds.

Writes stage under `.databridge-{transfer-id}.part` and publish
only on success. A handled failure removes its stage and leaves the existing
destination unchanged. After a process crash, startup marks a `running` or
`publishing` record `failed`, and its message says whether the destination may
already hold the new file; an unpublished stage may remain. Inspect the failed
transfer ID before manually removing its matching `.part` file from the
configured root. The one-process database rule is intentional: another worker
could otherwise mark a live transfer as interrupted, so a second process fails
at startup.

`make check` runs Ruff (including its security rules), strict mypy, and the
local/API tests, which include SFTP tests against an in-process loopback server
with injected faults. `make integration` runs the shared connector contract and
Docker SFTP tests. `make smoke` is a live HTTP reviewer
path. `make benchmark` checks SHA-256 and measures 16 MiB and 256 MiB transfers
both ways, plus 16 MiB through a relay that adds a 20 ms round trip. The
[measured results](docs/benchmark.md) include throughput, peak
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
alternative. Transfers are synchronous and do not resume after interruption. A
transfer copies the bytes the source held when it was opened; a source that
becomes shorter meanwhile fails with `SOURCE_CHANGED`, but an in-place rewrite
of the same length is not detected. Staging and collision behavior were tested
against the supplied SFTP server and a loopback server that imitates OpenSSH; a
supported server reports file sizes, answers unknown extensions with "operation
unsupported", and supports `posix-rename@openssh.com` for overwrite. SFTP cannot
make a new directory entry durable, so that depends on the server's filesystem.
Key rotation, background jobs, and concurrent-load testing are reasonable
follow-ups. No extra connector type or idempotency layer was added.
