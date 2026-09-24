# Large-file transfer measurement

Run `make benchmark` after the README's Docker and known-hosts setup. The script
creates deterministic binary files, transfers each one local→SFTP→local through
the HTTP app, compares SHA-256 at all three locations, and removes the generated
files. The raw run is in [benchmark-results.json](benchmark-results.json).

| Size | Upload | Download | Peak Python process RSS after case |
| --- | ---: | ---: | ---: |
| 16 MiB | 0.969 s · 16.52 MiB/s | 1.265 s · 12.65 MiB/s | 72.44 MiB |
| 256 MiB | 7.969 s · 32.12 MiB/s | 7.469 s · 34.27 MiB/s | 72.44 MiB |

Both files matched SHA-256 after upload and download. Before transfers, the
process's lifetime peak RSS was 62.48 MiB. After the 256 MiB cases it was
72.44 MiB, a 9.96 MiB increase. That is consistent with bounded-memory copying
at the configured 1 MiB chunk size. Throughput did not fall as file size grew
in this run; fixed connection and finalization costs matter more for the small
case.

These are single runs with the committed lockfile (cryptography 50.0.1 and
Paramiko 5.0.0) on macOS 26.5.1 arm64, Python 3.12.12, Docker Desktop's
arm64 engine, and an `atmoz/sftp:alpine-3.7` linux/amd64 container running under
emulation. The SFTP path is loopback and the remote directory is a Docker bind
mount on the same machine. `resource.ru_maxrss` is a process-lifetime high-water
mark, so each row reports the peak observed *by that point*, not an isolated
per-transfer allocation. Timings include SSH setup, copying, and publication;
they exclude file generation and hash verification. No network latency or
concurrent load was simulated.

As an **untested estimate**, a 1 GiB file at the measured 256 MiB rates would
take roughly 30–32 seconds per direction, plus variability in disk, network,
server, and SSH behavior. A practical request limit is more likely to be
available disk space and the caller's HTTP timeout than Python memory. Transfers
are synchronous and do not resume after interruption. The 256 MiB result is the
largest verified size, not a promise that arbitrary file sizes or SFTP servers
have the same throughput.
