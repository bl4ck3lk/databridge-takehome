# DataBridge

Python HTTP service for local and SFTP file connections and transfers. Implementation is in progress. The [exercise brief](instructions/README.md), [intent](intent.md), [requirements](spec.md), and [evidence map](verification.md) record the scope and decisions.

## Local setup

Use Python 3.12 and [uv](https://docs.astral.sh/uv/). Run `uv sync --extra dev`, then `make check`. Generate a stable, local encryption key once:

```bash
uv run python -c 'from cryptography.fernet import Fernet; print("DATABRIDGE_ENCRYPTION_KEY=" + Fernet.generate_key().decode())' > .env
```

Keep `.env` across service restarts. It is gitignored; losing it makes stored SFTP passwords unusable. Start and trust the local SFTP fixture:

```bash
mkdir -p sftp_data
docker compose up -d
ssh-keyscan -T 5 -p 2222 127.0.0.1 > known_hosts
chmod 600 known_hosts
make integration
```

Capturing the key presented at `127.0.0.1:2222` is an explicit trust-on-first-use step for this local fixture. The application rejects any other key. `make integration` checks real SFTP read, write, overwrite, collision, transfers, preview, and error cases. Start the API with `make run`; it serves `/openapi.json` and `/docs`. Connection creation, inspection, local/SFTP listing, bounded CSV/JSON preview, and synchronous transfers are implemented.

The root Compose file preserves generated SSH host keys in a named volume across normal container recreation. `docker compose down -v` deletes that volume and requires a new local known-hosts bootstrap. This image is amd64 and runs under emulation on ARM hosts; benchmark results must identify the platform.
