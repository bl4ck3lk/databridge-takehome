# DataBridge agent working rules

Read `instructions/README.md`, `intent.md`, `spec.md`, and `verification.md` before changing behavior. The supplied brief is the submission authority; `spec.md` records this project's chosen contract. If they conflict, resolve the conflict in the documents before implementing it.

- Name the affected R1–R14 requirements in the work summary. Change the spec and verification row in the same commit as any intentional behavior change. Do not silently drop a committed requirement.
- Build working vertical slices with focused commits. Add a file when it has a purpose and executable behavior; avoid empty placeholder modules or tests that merely restate the implementation.
- Keep HTTP validation and error mapping in the API layer, persistence and encryption in the store boundary, file operations in connectors, and byte-copy orchestration in the transfer service. The transfer service must not branch on connector type or parse file formats.
- Use the real Docker SFTP server for integration evidence. Tests must prove their setup exercised the guarded behavior: inspect persisted ciphertext, use a third test connector for transfer, and inject a failure after a chunk is written.
- Run and report the relevant checks from `verification.md`; a skipped or unavailable check is not a pass. Record platform and emulation when reporting transfer performance. Keep the README's fresh-checkout commands accurate.
- Keep `docs/ai-work-log.md` factual as work lands: user direction, AI-produced artifacts, checks, and corrections. Generate the brief's required `ai-collaboration-summary.md` from that evidence at the end.
- Never commit `.env`, encryption keys, SQLite databases, known-hosts material, generated SFTP data, or benchmark payloads. Do not print passwords or keys in logs, test failures, or collaboration summaries.

This is a local take-home service. Keep agent process lightweight and explainable; the working API and its evidence matter more than additional process files.
