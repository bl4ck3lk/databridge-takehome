# DataBridge

Python HTTP service for local and SFTP file connections and transfers. Implementation is in progress. The [exercise brief](instructions/README.md), [intent](intent.md), [requirements](spec.md), and [evidence map](verification.md) record the scope and decisions.

## Local setup

Use Python 3.12 and [uv](https://docs.astral.sh/uv/). Run `uv sync --extra dev`, then `make check`. Generate a stable, local encryption key once:

```bash
uv run python -c 'from cryptography.fernet import Fernet; print("DATABRIDGE_ENCRYPTION_KEY=" + Fernet.generate_key().decode())' > .env
```

Keep `.env` across service restarts. It is gitignored; losing it makes stored SFTP passwords unusable. Start the supplied SFTP fixture with `docker compose up -d` and the API with `make run`. The API serves `/openapi.json` and `/docs`. Connection creation, inspection, and local listing are implemented; SFTP file access, preview, and transfers follow in subsequent commits.

The root Compose file preserves generated SSH host keys in a named volume across normal container recreation. `docker compose down -v` deletes that volume and requires a new local known-hosts bootstrap. This image is amd64 and runs under emulation on ARM hosts; benchmark results must identify the platform.
