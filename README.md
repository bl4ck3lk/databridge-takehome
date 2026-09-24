# DataBridge

Python HTTP service for local and SFTP file connections and transfers. Implementation is in progress. The [exercise brief](instructions/README.md), [intent](intent.md), [requirements](spec.md), and [evidence map](verification.md) record the scope and decisions.

## Local scaffold

Use Python 3.12 and [uv](https://docs.astral.sh/uv/). Run `uv sync --extra dev`, then `make check`. Start the supplied SFTP fixture with `docker compose up -d` and the API scaffold with `make run`. The API currently serves `/openapi.json` and `/docs`; connection and transfer routes will arrive in subsequent commits.

The root Compose file preserves generated SSH host keys in a named volume across normal container recreation. `docker compose down -v` deletes that volume and requires a new local known-hosts bootstrap. This image is amd64 and runs under emulation on ARM hosts; benchmark results must identify the platform.
