"""Command line: `serve` runs the API on 127.0.0.1; `request-log-path` prints where it logs.

Both read their settings from the environment. A setting or database problem prints one line
that names the fix and exits with status 1; no server starts with a bad configuration.
"""

import argparse
import sys
from contextlib import ExitStack

import uvicorn

from databridge.api import open_app
from databridge.config import Settings
from databridge.errors import StartupError

HOST = "127.0.0.1"
DEFAULT_PORT = 8080


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m databridge", description="Run the DataBridge API or locate its log."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help=f"serve the API on {HOST}")
    serve.add_argument("--port", type=_tcp_port, default=DEFAULT_PORT, help="default: %(default)s")
    commands.add_parser("request-log-path", help="print the JSON-line request log's path")
    arguments = parser.parse_args(argv)
    if arguments.command == "serve":
        _serve(arguments.port)
    else:
        _print_request_log_path()


def _tcp_port(value: str) -> int:
    # ASCII digits only: int() also reads other scripts' digits, such as "٨٠" for 80.
    if not (value.isascii() and value.isdigit() and 1 <= int(value) <= 65535):
        raise argparse.ArgumentTypeError("must be a TCP port from 1 to 65535")
    return int(value)


def _serve(port: int) -> None:
    with ExitStack() as stack:
        try:
            app = stack.enter_context(open_app(Settings.from_env()))
        except StartupError as exc:
            sys.exit(f"DataBridge cannot start: {exc}")
        # The app object runs in this process only: one process owns the database.
        uvicorn.run(app, host=HOST, port=port, access_log=False)


def _print_request_log_path() -> None:
    try:
        settings = Settings.from_env()
    except StartupError as exc:
        sys.exit(f"Cannot locate the request log: {exc}")
    print(settings.request_log_path)


if __name__ == "__main__":
    main()
