.DEFAULT_GOAL := check

.PHONY: quickstart check integration benchmark smoke run logs

quickstart:
	@sh scripts/quickstart.sh

check:
	uv run --extra dev ruff check .
	uv run --extra dev ruff format --check .
	uv run --extra dev pytest -m "not integration"

# Targets that use the Docker SFTP fixture read its port and trust file from .env.
require_env = @test -f .env || { echo "Run make quickstart first; it writes .env" >&2; exit 1; }

integration:
	$(require_env)
	uv run --env-file .env --extra dev pytest -m integration

benchmark:
	$(require_env)
	@uv run --env-file .env --extra dev python scripts/benchmark.py

smoke:
	$(require_env)
	@uv run --env-file .env --extra dev python scripts/smoke.py

run:
	@printf '\nStarting DataBridge\n  API      http://127.0.0.1:8080\n  Docs     http://127.0.0.1:8080/docs\n  OpenAPI  http://127.0.0.1:8080/openapi.json\n  Logs     make logs (in another terminal)\n\n'
	@uv run --env-file .env uvicorn databridge.api:app --host 127.0.0.1 --port 8080 --workers 1 --no-access-log

logs:
	@tail -n 50 -F "$$(uv run --env-file .env python -c 'from databridge.config import Settings; print(Settings.from_env().request_log_path)')"
