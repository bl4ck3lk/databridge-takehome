.PHONY: check integration benchmark run

check:
	uv run --extra dev ruff check .
	uv run --extra dev ruff format --check .
	uv run --extra dev pytest -m "not integration"

integration:
	uv run --extra dev pytest -m integration

benchmark:
	@uv run --extra dev python scripts/benchmark.py

run:
	uv run --env-file .env uvicorn databridge.api:app --host 127.0.0.1 --port 8080 --workers 1
