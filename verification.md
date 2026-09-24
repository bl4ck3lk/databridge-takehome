# Requirement evidence map

Status is **planned** until the named behavior has passed against the implementation. Update this table with the exact test or artifact as each slice lands. `make check` will run static checks and local/API tests; `make integration` will use Docker SFTP; the live smoke command and benchmark will run separately. These commands do not exist yet.

| ID | Acceptance evidence | Status |
| --- | --- | --- |
| R1 | API create/get and service restart using the same temporary SQLite file | Planned |
| R2 | Local and SFTP listing, including missing connection and both enumeration caps | Planned |
| R3 | Local CSV and JSON previews against supplied fixtures | Planned |
| R4 | Real Docker SFTP connection and listing through the shared connector contract | Planned |
| R5 | Byte-identical transfers both ways; a third test connector; collision and same-file cases | Planned |
| R6 | Completed and failed records, failure after one chunk, destination preservation, startup recovery | Planned |
| R7 | API assertions for the error-code table, including bad credentials and server down | Planned |
| R8 | Deterministic CSV/JSON inference cases through local and SFTP connectors | Planned |
| R9 | Healthcheck success, authentication failure, unavailable server, and root access failure | Planned |
| R10 | Reproducible 16 MiB and 256 MiB transfers, SHA-256, time, throughput, peak RSS, platform | Planned |
| R11 | Shared parameterized connector scenarios and Docker integration, including boundary failures | Planned |
| R12 | Live `/openapi.json` and `/docs`, checked against documented main routes | Planned |
| R13 | SQLite ciphertext inspection, response redaction, same-key restart, missing/wrong-key startup | Planned |
| R14 | One fresh-checkout smoke command against live HTTP and Docker SFTP with hash comparison | Planned |

Passing local tests does not establish the Docker or live HTTP path. A benchmark is measured evidence on its stated machine, not a universal throughput guarantee. The final README will point to the exact commands and results.
