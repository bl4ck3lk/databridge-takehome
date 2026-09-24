"""HTTP application entry point."""

from fastapi import FastAPI


def create_app() -> FastAPI:
    """Build an app instance so tests can control its lifecycle and settings."""
    return FastAPI(title="DataBridge", version="0.1.0")


app = create_app()
