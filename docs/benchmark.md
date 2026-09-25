# Large-file transfer measurement

Run `make benchmark` after `make quickstart` has started the fixture. The script
creates deterministic binary files, transfers each one local→SFTP→local through
the HTTP app, compares SHA-256 at all three locations, and removes the generated
files. It then repeats the first size through a loopback relay that delays each
direction by half of a 20 ms round trip without limiting bandwidth, because
loopback hides the cost of waiting for replies. The raw run is in
[benchmark-results.json](benchmark-results.json).

| Case | Upload | Download | Peak Python process RSS after case |
| --- | ---: | ---: | ---: |
| 16 MiB | 0.901 s · 17.75 MiB/s | 0.842 s · 19.00 MiB/s | 70.95 MiB |
| 256 MiB | 2.955 s · 86.64 MiB/s | 4.011 s · 63.83 MiB/s | 70.97 MiB |
| 16 MiB, 20 ms round trip | 0.996 s · 16.06 MiB/s | 0.835 s · 19.17 MiB/s | 70.97 MiB |

Every file matched SHA-256 after upload and download. Before transfers, the
process's lifetime peak RSS was 64.31 MiB. After the 256 MiB
case it was 70.97 MiB, a
6.66 MiB increase, and it stayed there for the later latency case. This is
consistent with bounded-memory copying: 1 MiB chunks plus up to 64 SFTP
requests of 32 KiB in flight per file.

**Why the SFTP connector pipelines requests.** For comparison, the same machine
ran the 16 MiB cases once with the connector temporarily limited to one request
in flight, which is how it copied before the review
([raw run](benchmark-one-request-results.json)):

| 16 MiB case | One request in flight | 64 requests in flight |
| --- | ---: | ---: |
| Loopback upload | 17.39 MiB/s | 17.75 MiB/s |
| Loopback download | 19.35 MiB/s | 19.00 MiB/s |
| 20 ms round trip, upload | 1.22 MiB/s (13.15 s) | 16.06 MiB/s (1.00 s) |
| 20 ms round trip, download | 1.26 MiB/s (12.75 s) | 19.17 MiB/s (0.83 s) |

With one request in flight, every 32 KiB waits a full round trip. The
one-request run was on earlier commit `3fe79dd`; the 64-request numbers above
are the current run on `39a064e`. This comparison illustrates the size of the
latency effect, but is not a same-commit controlled experiment.

The current results are single runs from `make benchmark` on code commit `39a064e` with the
committed lockfile (cryptography 50.0.1 and Paramiko 5.0.0) on macOS 26.5.1
arm64, Python 3.12.12,
Docker Desktop's arm64 engine, and an `atmoz/sftp:alpine-3.7` linux/amd64
container running under emulation. The SFTP path is loopback plus the optional
relay, and the remote directory is a Docker bind mount on the same machine.
`resource.ru_maxrss` is a process-lifetime high-water mark, so each row reports
the peak observed *by that point*, not an isolated per-transfer allocation.
Timings include SSH setup, copying, and publication; they exclude file
generation and hash verification. No concurrent load was simulated.

As an **untested estimate**, a 1 GiB file at the measured 256 MiB rates would
take roughly 12–16 seconds per direction, plus variability in disk, network,
server, and SSH behavior. A practical request limit is more likely to be
available disk space and the caller's HTTP timeout than Python memory. Transfers
are synchronous and do not resume after interruption. The 256 MiB result is the
largest verified size, not a promise that arbitrary file sizes or SFTP servers
have the same throughput.
