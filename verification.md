# Requirement evidence map

Status is **planned**, **partial**, or **verified**. Update this table with the exact test or artifact as each slice lands. `make check` runs static checks and local/API tests; `make integration` will use Docker SFTP; the live smoke command and benchmark will run separately. The latter commands do not exist yet.

| ID | Acceptance evidence | Status |
| --- | --- | --- |
| R1 | `test_local_connection_persists_and_lists_files` creates, lists, and reopens the same SQLite file | Verified |
| R2 | Local listing and caps: `test_local_connection_persists_and_lists_files`, `test_local_listing_reports_result_and_scan_caps`; SFTP listing: `test_sftp_roundtrip_collision_and_explicit_overwrite`; SFTP caps pending | Partial |
| R3 | Local CSV and JSON previews against supplied fixtures | Planned |
| R4 | `test_sftp_roundtrip_collision_and_explicit_overwrite` creates a stored connection and lists/reads/writes through the connector against Docker | Verified |
| R5 | Byte-identical transfers both ways; a third test connector; collision and same-file cases | Planned |
| R6 | Completed and failed records, failure after one chunk, destination preservation, startup recovery | Planned |
| R7 | Local invalid/duplicate/missing: `test_connection_errors_are_structured`; SFTP auth, unavailable server, root, and trust: `test_sftp_failures_have_distinct_safe_codes`; transfer errors pending | Partial |
| R8 | Deterministic CSV/JSON inference cases through local and SFTP connectors | Planned |
| R9 | Healthcheck success, authentication failure, unavailable server, and root access failure | Planned |
| R10 | Reproducible 16 MiB and 256 MiB transfers, SHA-256, time, throughput, peak RSS, platform | Planned |
| R11 | Local staging, collision, cleanup, and path escapes in `test_connections.py`; Docker SFTP read/write/overwrite/publish collision in `test_sftp_integration.py`; shared parameterized scenarios pending | Partial |
| R12 | `/openapi.json` served in `test_openapi_is_served` and a real Uvicorn request; main route contract pending | Partial |
| R13 | Ciphertext/redaction and wrong/missing key: `test_connections.py`; SFTP use after store restart: `test_sftp_roundtrip_collision_and_explicit_overwrite`; final log review pending | Partial |
| R14 | One fresh-checkout smoke command against live HTTP and Docker SFTP with hash comparison | Planned |

Passing local tests does not establish the Docker or live HTTP path. A benchmark is measured evidence on its stated machine, not a universal throughput guarantee. The final README will point to the exact commands and results.

2026-09-24 gates: `make check` with a writable temporary uv cache passed Ruff and 9 local/API tests; `make integration` passed 3 real Docker SFTP tests. The scaffold's live Uvicorn process returned HTTP 200 at `/openapi.json`; no connection or transfer route has yet been exercised over live HTTP.
