"""Preview shape, inference, and byte-budget behavior."""

from pathlib import Path

from fastapi.testclient import TestClient

from databridge.api import create_app
from databridge.config import Settings
from databridge.preview import MAX_PREVIEW_BYTES

FIXTURES = Path(__file__).resolve().parents[1] / "instructions"


def test_supplied_csv_and_json_preview(settings: Settings) -> None:
    with TestClient(create_app(settings)) as client:
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
    (tmp_path / "bad.json").write_text('[{"x":1},]')
    (tmp_path / "bad.csv").write_text("a,a\n1,2\n")
    (tmp_path / "bad-number.json").write_text('[{"x":NaN}]')
    (tmp_path / "other.txt").write_text("text")
    with TestClient(create_app(settings)) as client:
        client.post("/connections", json={"name": "files", "type": "local", "path": str(tmp_path)})
        csv_preview = client.get("/connections/files/files/mixed.csv/head?limit=10")
        json_preview = client.get("/connections/files/files/mixed.json/head?limit=10")
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
    assert bad_json.json()["error"]["code"] == "MALFORMED_FILE"
    assert bad_json_short.json()["error"]["code"] == "MALFORMED_FILE"
    assert bad_csv.json()["error"]["code"] == "MALFORMED_FILE"
    assert bad_number.json()["error"]["code"] == "MALFORMED_FILE"
    assert unsupported.json()["error"]["code"] == "UNSUPPORTED_PREVIEW_FORMAT"
    assert invalid_limit.status_code == 422
    assert invalid_limit.json()["error"]["code"] == "INVALID_REQUEST"


def test_preview_rejects_a_record_beyond_byte_budget(settings: Settings, tmp_path: Path) -> None:
    (tmp_path / "wide.csv").write_bytes(b"field\n" + b"x" * MAX_PREVIEW_BYTES + b"\n")
    with TestClient(create_app(settings)) as client:
        client.post("/connections", json={"name": "files", "type": "local", "path": str(tmp_path)})
        response = client.get("/connections/files/files/wide.csv/head")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "PREVIEW_LIMIT_EXCEEDED"
