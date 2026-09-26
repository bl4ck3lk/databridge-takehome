# AI collaboration summary

I was the primary AI collaborator, using Codex with shell tools, Git, uv,
pytest, and Docker Compose. The candidate chose Python, set the scope, supplied
two outside reviews, and corrected the plan and the running service during the
work. I drafted the intent, requirements, agent rules, code, tests, benchmark,
smoke check, and documentation. The repository code and prose I wrote are
AI-generated; the candidate provided the direction and reviewed the results.

We worked from the supplied brief, then implemented the service in focused
commits. I used FastAPI, a small connector protocol, SQLite with
Fernet-encrypted SFTP passwords, staged writes, and a transfer service that
does not branch on connector type. Checks included Ruff, pytest, the supplied
Docker SFTP server, live HTTP requests, and hash comparisons. The private GitHub
repository preserves the implementation history; publication is the candidate's
decision.

Three examples of direction and output:

1. “let's capture the intent and then create requirements docs” produced
   `intent.md` and `spec.md`. The candidate then added Phase 3 and credential
   encryption from the start. The outside reviews exposed missing decisions
   about same-file copies, interrupted transfers, and the SFTP fixture, which I
   incorporated into the spec.
2. “success should also include the transfer of large files without performance
   degradation or at least an informed estimate of limits” led to 1 MiB chunked
   copies and a reproducible 16/256 MiB test in both directions. The benchmark
   notes report hashes, throughput, process peak memory, and the emulated SFTP
   container; their 1 GiB projection is explicitly untested.
3. “could we have a way of you watching the logs live and capture any errors
   with its params” led to redacted request events and `make logs`. A live replay
   showed that Swagger's literal `"string"` path, rather than the shell-variable
   explanation I had guessed, caused the confusing connection result. I added
   concrete API examples and recorded the correction in `docs/ai-work-log.md`.

Docker integration also caught my initial Paramiko write-mode error. The
candidate asked for a trail of work and for this report to be written together;
the log records those corrections and the checks behind them.

Other tools: the text above is Codex's response to the brief's prompt, edited
with the candidate. Afterwards the candidate used Claude Code to review the
repository and to implement the review's findings on the `fix/tribunal-findings`
branch; `docs/ai-work-log.md` records that work and its checks.

Codex later rechecked PR #1, reproduced seven remaining issues at its hosted head,
then added regression tests and fixes with the candidate's authorization. The
work log and verification map record the follow-up checks and benchmark.

Claude Code then checked an outside review of the merged work. It confirmed four
of the seven findings in whole or in part and fixed them test-first. The work
log records why the other three did not hold.
