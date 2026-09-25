"""Preview shape, inference, and byte-budget behavior."""

import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from support import client_for

from databridge.config import Settings
from databridge.connectors.base import AccessCheck, Listing
from databridge.errors import DataBridgeError, ErrorCode
from databridge.preview import MAX_PREVIEW_BYTES, preview

FIXTURES = Path(__file__).resolve().parents[1] / "instructions"


class _Trickle:
    """A source that returns at most seven bytes per read, as the ByteSource contract allows."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def read(self, size: int, /) -> bytes:
        chunk = self.data[self.offset : self.offset + min(size, 7)]
        self.offset += len(chunk)
        return chunk


class _TrickleConnector:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files

    def list_files(self) -> Listing:
        return Listing(sorted(self.files), False)

    def check_access(self) -> AccessCheck:
        return AccessCheck(writable=False)

    @contextmanager
    def read(self, filename: str, limit: int | None = None) -> Iterator[_Trickle]:
        yield _Trickle(self.files[filename][:limit])

    @contextmanager
    def write(self, filename: str, staging_id: str, overwrite: bool) -> Iterator[Any]:
        raise NotImplementedError("read-only test connector")
        yield


def _head(settings: Settings, folder: Path, filename: str, limit: int = 5) -> Any:
    with client_for(settings) as client:
        created = client.post(
            "/connections", json={"name": "files", "type": "local", "path": str(folder)}
        )
        assert created.status_code in {201, 409}  # 409: an earlier call created it
        return client.get(f"/connections/files/files/{filename}/head", params={"limit": limit})


def test_preview_fills_its_budget_from_short_reads() -> None:
    rows = "".join(f"{index},name-{index}\n" for index in range(10))
    connector = _TrickleConnector({"data.csv": ("id,name\n" + rows).encode()})
    result = preview(connector, "data.csv", 10)
    assert [row["id"] for row in result.rows] == [str(index) for index in range(10)]
    over_budget = _TrickleConnector({"wide.csv": b"field\n" + b"x" * MAX_PREVIEW_BYTES + b"\n"})
    with pytest.raises(DataBridgeError) as error:
        preview(over_budget, "wide.csv", 1)
    assert error.value.code == ErrorCode.PREVIEW_LIMIT_EXCEEDED


def test_csv_field_longer_than_the_csv_module_default(settings: Settings, tmp_path: Path) -> None:
    long_value = "v" * 200_000
    (tmp_path / "long.csv").write_text(f"id,text\n1,{long_value}\n")
    response = _head(settings, tmp_path, "long.csv")
    assert response.status_code == 200
    assert response.json()["rows"] == [{"id": "1", "text": long_value}]


def test_csv_blank_lines_are_skipped(settings: Settings, tmp_path: Path) -> None:
    (tmp_path / "blank.csv").write_bytes(b"a,b\n\n1,2\n\r\n3,4\n\n")
    response = _head(settings, tmp_path, "blank.csv")
    assert response.status_code == 200
    assert response.json()["rows"] == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


def test_csv_values_keep_unicode_separators_and_quoted_newlines(
    settings: Settings, tmp_path: Path
) -> None:
    (tmp_path / "separators.csv").write_bytes(
        'name,note\nx,a b\ny,c\x85d\x0ce\nz,"first\nsecond"\n'.encode()
    )
    response = _head(settings, tmp_path, "separators.csv")
    assert response.status_code == 200
    assert [row["note"] for row in response.json()["rows"]] == [
        "a b",
        "c\x85d\x0ce",
        "first\nsecond",
    ]


def test_csv_record_cut_by_the_byte_limit_is_never_returned(
    settings: Settings, tmp_path: Path
) -> None:
    prefix = b"field\n" + (b"x" * 10_000 + b"\n") * 98
    cut_row = b"y" * 100 + " ".encode() + b"z" * (MAX_PREVIEW_BYTES - len(prefix)) + b"\n"
    (tmp_path / "cut.csv").write_bytes(prefix + cut_row)
    response = _head(settings, tmp_path, "cut.csv", limit=99)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "PREVIEW_LIMIT_EXCEEDED"


@pytest.mark.parametrize(
    ("document", "code"),
    [
        ("[" * 100_000 + "]" * 100_000, "PREVIEW_LIMIT_EXCEEDED"),
        ('[{"a":' + "[" * 40 + "]" * 40 + "}]", "PREVIEW_LIMIT_EXCEEDED"),
        ('[{"a":"\\ud800"}]', "MALFORMED_FILE"),
        ('[{"\\udc00":1}]', "MALFORMED_FILE"),
        ('[{"a":1e400}]', "MALFORMED_FILE"),
        ('[{"a":' + "1" * 5_000 + "}]", "MALFORMED_FILE"),
    ],
    ids=[
        "deep-array",
        "deep-row",
        "lone-surrogate-value",
        "lone-surrogate-key",
        "overflow",
        "digits",
    ],
)
@pytest.mark.parametrize("padded", [False, True], ids=["complete", "over-budget"])
def test_hostile_json_is_rejected_with_a_preview_error(
    settings: Settings, tmp_path: Path, document: str, code: str, padded: bool
) -> None:
    if padded:
        document = document[:-1] + ',{"pad":"' + "p" * MAX_PREVIEW_BYTES + '"}]'
    (tmp_path / "hostile.json").write_text(document)
    response = _head(settings, tmp_path, "hostile.json", limit=1)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == code


SMALL_STACK_PREVIEW = """
import sys, threading
from pathlib import Path
from databridge.connectors.local import LocalConnector
from databridge.errors import DataBridgeError
from databridge.preview import preview

codes = []

def run():
    try:
        preview(LocalConnector(Path(sys.argv[1])), "deep.json", 1)
    except DataBridgeError as exc:
        codes.append(exc.code)

threading.stack_size(128 * 1024)
thread = threading.Thread(target=run)
thread.start()
thread.join()
print(codes)
"""


@pytest.mark.parametrize("padded", [False, True], ids=["complete", "over-budget"])
def test_deep_json_is_refused_on_a_small_thread_stack(tmp_path: Path, padded: bool) -> None:
    """musl gives each thread 128 KiB of stack, and the API previews in a worker thread; the
    recursive JSON decoder must never reach deep nesting there."""
    document = "[" * 100_000 + "]" * 100_000
    if padded:
        document = document[:-1] + ',{"pad":"' + "p" * MAX_PREVIEW_BYTES + '"}]'
    (tmp_path / "deep.json").write_text(document)
    result = subprocess.run(
        [sys.executable, "-c", SMALL_STACK_PREVIEW, str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "[<ErrorCode.PREVIEW_LIMIT_EXCEEDED: 'PREVIEW_LIMIT_EXCEEDED'>]\n"


def test_depth_is_checked_in_every_row_of_a_complete_file(
    settings: Settings, tmp_path: Path
) -> None:
    deep_row = '{"a":' + "[" * 40 + "]" * 40 + "}"
    (tmp_path / "rows.json").write_text(f'[{{"a":1}}, {deep_row}]')
    response = _head(settings, tmp_path, "rows.json", limit=1)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "PREVIEW_LIMIT_EXCEEDED"


PAD = "p" * MAX_PREVIEW_BYTES


@pytest.mark.parametrize(
    ("document", "code"),
    [
        ('[{"a": tru}, {"pad": "' + PAD + '"}]', "MALFORMED_FILE"),
        ('[{"a": 1} {"b": 2}, {"pad": "' + PAD + '"}]', "MALFORMED_FILE"),
        ('[{"a": "\\q"}, {"pad": "' + PAD + '"}]', "MALFORMED_FILE"),
        ('[{"pad": "' + PAD + '"}]', "PREVIEW_LIMIT_EXCEEDED"),
    ],
    ids=["bad-literal", "missing-comma", "bad-escape", "string-cut-by-budget"],
)
def test_json_past_the_budget_tells_errors_from_truncation(
    settings: Settings, tmp_path: Path, document: str, code: str
) -> None:
    (tmp_path / "large.json").write_text(document)
    response = _head(settings, tmp_path, "large.json", limit=2)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == code


def test_type_inference_uses_ascii_grammars_and_strict_dates(
    settings: Settings, tmp_path: Path
) -> None:
    (tmp_path / "grammar.csv").write_text(
        "underscored,arabic,compact,week,exponent,overflow\n"
        "1_000,١٢٣,20240101,2024-W01-1,1e5,1e400\n"
    )
    (tmp_path / "grammar.json").write_text(
        '[{"compact":"20240101","week":"2024-W01-1","day":"2024-01-31","invalid":"2024-02-30"}]'
    )
    csv_schema = _head(settings, tmp_path, "grammar.csv").json()["schema"]
    json_schema = _head(settings, tmp_path, "grammar.json").json()["schema"]
    assert csv_schema == {
        "underscored": "string",
        "arabic": "string",
        "compact": "integer",
        "week": "string",
        "exponent": "float",
        "overflow": "string",
    }
    assert json_schema == {
        "compact": "string",
        "week": "string",
        "day": "date",
        "invalid": "string",
    }


def test_supplied_csv_and_json_preview(settings: Settings) -> None:
    with client_for(settings) as client:
        created = client.post(
            "/connections", json={"name": "fixtures", "type": "local", "path": str(FIXTURES)}
        )
        assert created.status_code == 201
        csv_response = client.get("/connections/fixtures/files/customer.csv/head")
        json_response = client.get("/connections/fixtures/files/products.json/head?limit=2")

    assert csv_response.status_code == 200
    csv_preview = csv_response.json()
    assert len(csv_preview["rows"]) == 5
    assert csv_preview["rows"][0]["age"] == "34"
    assert csv_preview["schema"] == {
        "id": "integer",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "age": "integer",
        "signup_date": "date",
        "is_active": "boolean",
        "balance": "float",
    }
    assert json_response.status_code == 200
    json_preview = json_response.json()
    assert len(json_preview["rows"]) == 2
    assert json_preview["rows"][0]["product_id"] == 101
    assert json_preview["schema"] == {
        "product_id": "integer",
        "name": "string",
        "category": "string",
        "price": "float",
        "in_stock": "boolean",
    }


def test_preview_inference_empty_mixed_and_malformed(settings: Settings, tmp_path: Path) -> None:
    (tmp_path / "mixed.csv").write_text("count,date,flag,blank\n1,2024-01-01,true,\n2.5,,false,\n")
    (tmp_path / "mixed.json").write_text(
        '[{"count":1,"flag":true,"nullable":null},{"count":2.5,"flag":false,"nullable":"x"}]'
    )
    (tmp_path / "json-strings.json").write_text(
        '[{"numeric":"42","boolean":"true","date":"2024-01-01"}]'
    )
    (tmp_path / "bad.json").write_text('[{"x":1},]')
    (tmp_path / "bad.csv").write_text("a,a\n1,2\n")
    (tmp_path / "bad-number.json").write_text('[{"x":NaN}]')
    (tmp_path / "other.txt").write_text("text")
    with client_for(settings) as client:
        client.post("/connections", json={"name": "files", "type": "local", "path": str(tmp_path)})
        csv_preview = client.get("/connections/files/files/mixed.csv/head?limit=10")
        json_preview = client.get("/connections/files/files/mixed.json/head?limit=10")
        json_strings = client.get("/connections/files/files/json-strings.json/head")
        bad_json = client.get("/connections/files/files/bad.json/head?limit=10")
        bad_json_short = client.get("/connections/files/files/bad.json/head?limit=1")
        bad_csv = client.get("/connections/files/files/bad.csv/head")
        bad_number = client.get("/connections/files/files/bad-number.json/head")
        unsupported = client.get("/connections/files/files/other.txt/head")
        invalid_limit = client.get("/connections/files/files/mixed.csv/head?limit=101")

    assert csv_preview.status_code == 200
    assert csv_preview.json()["schema"] == {
        "count": "float",
        "date": "date",
        "flag": "boolean",
        "blank": "string",
    }
    assert json_preview.status_code == 200
    assert json_preview.json()["schema"] == {
        "count": "float",
        "flag": "boolean",
        "nullable": "string",
    }
    assert json_strings.json()["schema"] == {
        "numeric": "string",
        "boolean": "string",
        "date": "date",
    }
    assert bad_json.json()["error"]["code"] == "MALFORMED_FILE"
    assert bad_json_short.json()["error"]["code"] == "MALFORMED_FILE"
    assert bad_csv.json()["error"]["code"] == "MALFORMED_FILE"
    assert bad_number.json()["error"]["code"] == "MALFORMED_FILE"
    assert unsupported.json()["error"]["code"] == "UNSUPPORTED_PREVIEW_FORMAT"
    assert invalid_limit.status_code == 422
    assert invalid_limit.json()["error"]["code"] == "INVALID_REQUEST"


def test_preview_rejects_a_record_beyond_byte_budget(settings: Settings, tmp_path: Path) -> None:
    (tmp_path / "wide.csv").write_bytes(b"field\n" + b"x" * MAX_PREVIEW_BYTES + b"\n")
    with client_for(settings) as client:
        client.post("/connections", json={"name": "files", "type": "local", "path": str(tmp_path)})
        response = client.get("/connections/files/files/wide.csv/head")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "PREVIEW_LIMIT_EXCEEDED"


def test_preview_accepts_exact_byte_budget_without_final_newline(
    settings: Settings, tmp_path: Path
) -> None:
    prefix = b"field\n" + (b"x" * 10_000 + b"\n") * 99
    final_row = b"y" * (MAX_PREVIEW_BYTES - len(prefix))
    (tmp_path / "exact.csv").write_bytes(prefix + final_row)
    with client_for(settings) as client:
        client.post("/connections", json={"name": "files", "type": "local", "path": str(tmp_path)})
        response = client.get("/connections/files/files/exact.csv/head?limit=100")
    assert response.status_code == 200
    assert len(response.json()["rows"]) == 100
    assert response.json()["rows"][-1]["field"] == final_row.decode()
