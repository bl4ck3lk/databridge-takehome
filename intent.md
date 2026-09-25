# Intent: DataBridge take-home

Status: current scope
Originator: candidate for the senior engineer, data platform role  
Captured: 2026-09-24

## Problem

The take-home asks for a web service that stores reusable connections, lists and previews files, and transfers files between a local directory and an SFTP server. The exercise evaluates the connector abstraction, working bidirectional transfers, persistent connection state, clear failures, and code the candidate can explain. The supplied brief is the authority for the submission requirements: [instructions/README.md](instructions/README.md).

## Desired outcome

Submit a Python service that a reviewer can start and exercise against the supplied Docker SFTP server. Complete all three phases, including CSV/JSON schema preview and polished errors. Encrypt persisted SFTP credentials from the initial storage design, and deliver a focused set of further bonuses that strengthen the same design. The result should demonstrate senior engineering judgment through simple boundaries, reliable data movement, useful tests, and honest documentation. The candidate should be able to explain every architectural choice and tradeoff in a debrief.

## Users and affected systems

- **Reviewer:** starts the service, creates connections, inspects files, runs transfers in both directions, checks status and failure responses, and reads the documentation and commit history.
- **Candidate:** implements, tests, and explains the solution and records how AI assisted.
- **Systems:** the Python HTTP service, its persistent local store, a configured local directory, and the supplied SFTP container.

## Constraints

- Python; HTTP API; the connection store survives service restarts.
- Both connectors implement the same list/read/write contract. Transfer orchestration remains independent of connector type and file format.
- The intended submission includes Phases 1, 2, and 3, plus credential encryption from the first persistent-connection implementation. Keep the encryption key outside the database and stable across restarts; never expose stored credentials in API responses or logs.
- Large-file transfer is part of success: stream in bounded memory and measure behavior at realistic file sizes, or give an evidence-based estimate of limits when an end-to-end large-file test is unavailable.
- Make transfer outcomes unambiguous: define what happens when the destination exists, avoid a provable copy onto the same file, publish complete files only, and report useful failure progress without exposing secrets.
- Bound listing and preview work, and prove connector behavior with small failure and boundary-case scenarios as well as a reviewer smoke path.
- Run one service process against its SQLite store. Make the local SFTP container's trust stable across normal recreation, verify write permissions from a fresh checkout, and state what a process crash can leave behind.
- Target the further bonuses of connection healthcheck, connector contract tests, and application-served API docs. An additional cloud connector is outside the intended scope.
- Keep external dependencies limited and the service easy to run locally. Document setup, API use, design decisions, cuts, and further improvements.
- Produce the required `ai-collaboration-summary.md` from the brief's exact prompt at submission time, with an accurate account of human and AI work.

## Success

A fresh checkout can follow the README to establish local SFTP trust and a writable test directory, then start the service and container; create and reuse connections after a service restart without persisting plaintext SFTP passwords; list and preview the provided CSV and JSON files with inferred schema; transfer files in both directions with matching bytes and observable status; and receive actionable errors for invalid input, missing files, bad credentials, and an unavailable server. A large file can also be transferred without memory use growing with file size or avoidable throughput collapse. The submission includes measured throughput and memory behavior at stated file sizes and on a stated platform, or an informed estimate of the practical limits with its assumptions and uncertainty. Existing destinations, failed copies, oversized listings, and a process interruption have explicit outcomes. Automated tests cover the connector contract, boundary cases, and important API paths, while one documented smoke command exercises the reviewer path. The submission remains small enough to walk through comfortably.

## Review points

- The service is intended for local, trusted use. Multiple callers can access the same shared connections and transfer records; there is no caller identity or tenant isolation. Public or multi-tenant access would require a separate authorization and isolation design.
