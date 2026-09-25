"""Quickstart's .env handling: values are stored literally and never run as shell."""

import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "env_file.sh"


def _shell(tmp_path: Path, *commands: str) -> subprocess.CompletedProcess[str]:
    """Run commands after loading the helpers; arguments are passed positionally, not quoted."""
    return subprocess.run(
        ["sh", "-c", f'. "$0"; {"; ".join(commands)}', str(SCRIPT)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_values_with_shell_characters_are_stored_and_read_literally(tmp_path: Path) -> None:
    value = "/work/$HOME/`touch pwned`/a b"
    (tmp_path / "value").write_text(value)
    result = _shell(tmp_path, 'ensure_env .env KEY "$(cat value)"', "read_env .env KEY > read-back")
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "read-back").read_text() == value + "\n"
    assert (tmp_path / ".env").read_text() == f"KEY='{value}'\n"
    assert not (tmp_path / "pwned").exists()


def test_an_existing_value_is_never_replaced(tmp_path: Path) -> None:
    result = _shell(tmp_path, "ensure_env .env KEY first", "ensure_env .env KEY second")
    assert result.returncode == 0, result.stderr
    assert (tmp_path / ".env").read_text() == "KEY='first'\n"


def test_a_value_with_a_single_quote_is_refused(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("")
    result = _shell(tmp_path, 'ensure_env .env KEY "it\'s"')
    assert result.returncode != 0
    assert "single quote" in result.stderr
    assert (tmp_path / ".env").read_text() == ""


def test_an_unquoted_value_is_read_as_written(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("KEY=plain-value\n")
    result = _shell(tmp_path, "read_env .env KEY")
    assert result.returncode == 0, result.stderr
    assert result.stdout == "plain-value\n"


def test_a_double_quoted_value_is_refused(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("KEY=\"/path\"\nOTHER='x'\n")
    result = _shell(tmp_path, "read_env .env KEY")
    assert result.returncode != 0
    assert result.stdout == ""
    assert "KEY='value'" in result.stderr
