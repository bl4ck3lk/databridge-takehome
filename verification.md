# Requirement evidence map

Status is **planned**, **partial**, or **verified**. Update this table with the exact test or artifact as each slice lands. `make check` runs static checks and local/API tests; `make integration` will use Docker SFTP; the live smoke command and benchmark will run separately. The latter commands do not exist yet.

| ID | Acceptance evidence | Status |
| --- | --- | --- |
| R1 | `test_local_connection_persists_and_lists_files` creates, lists, and reopens the same SQLite file | Verified |
| R2 | Local listing and caps: `test_local_connection_persists_and_lists_files`, `test_local_listing_reports_result_and_scan_caps`; SFTP listing: `test_sftp_roundtrip_collision_and_explicit_overwrite`; SFTP caps pending | Partial |
| R3 | `test_supplied_csv_and_json_preview` checks the supplied 15-row CSV and six-object JSON through the local API; preview row and byte bounds in `test_preview.py` | Verified |
| R4 | `test_sftp_roundtrip_collision_and_explicit_overwrite` creates a stored connection and lists/reads/writes through the connector against Docker | Verified |
| R5 | `test_api_transfers_binary_both_directions_and_preserves_collision` copies a binary file over a chunk in both directions and checks bytes; `test_third_connector_fails_after_chunk_without_publishing` uses a third connector; same-file and overwrite tests in `test_transfers.py` | Verified |
| R6 | `test_local_transfer_bytes_status_collision_and_overwrite`, `test_third_connector_fails_after_chunk_without_publishing`, `test_startup_marks_interrupted_transfer_failed`, and `test_api_failed_sftp_transfer_has_retrievable_record` check persisted outcomes and destination preservation; interrupted staging listing still pending | Partial |
| R7 | `test_connection_errors_are_structured`, `test_sftp_failures_have_distinct_safe_codes`, `test_transfer_rejects_same_file_and_reports_missing_source`, and `test_api_failed_sftp_transfer_has_retrievable_record` check stable errors and transfer IDs | Verified |
| R8 | `test_supplied_csv_and_json_preview`, `test_preview_inference_empty_mixed_and_malformed`, and `test_sftp_preview_uses_shared_parser` check types, empty/mixed values, malformed input, and SFTP parity | Verified |
| R9 | Local and SFTP healthcheck success; bad password, unavailable server, and missing root tested in `test_sftp_integration.py`. Explicit permission-denied fixture pending. | Partial |
| R10 | Reproducible 16 MiB and 256 MiB transfers, SHA-256, time, throughput, peak RSS, platform | Planned |
| R11 | Local staging, collision, cleanup, and path escapes in `test_connections.py`; Docker SFTP read/write/overwrite/publish collision in `test_sftp_integration.py`; shared parameterized scenarios pending | Partial |
| R12 | `/openapi.json` served in `test_openapi_is_served` and a real Uvicorn request; main route contract pending | Partial |
| R13 | Ciphertext/redaction and wrong/missing key: `test_connections.py`; SFTP use after store restart: `test_sftp_roundtrip_collision_and_explicit_overwrite`; final log review pending | Partial |
| R14 | One fresh-checkout smoke command against live HTTP and Docker SFTP with hash comparison | Planned |

Passing local tests does not establish the Docker or live HTTP path. A benchmark is measured evidence on its stated machine, not a universal throughput guarantee. The final README will point to the exact commands and results.

2026-09-24 gates: `make check` with a writable temporary uv cache passed Ruff and 17 local/API tests; `make integration` passed 6 real Docker SFTP tests. The scaffold's live Uvicorn process returned HTTP 200 at `/openapi.json`; transfer and preview routes have been exercised through FastAPI TestClient but not yet over live HTTP.
