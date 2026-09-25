"""Bounded CSV/JSON head previews through the connector read contract."""

import codecs
import csv
import io
import json
import math
import re
from datetime import date
from pathlib import PurePath
from typing import Any, Literal

from databridge.connectors.base import Connector, validate_filename
from databridge.errors import DataBridgeError, ErrorCode
from databridge.models import PreviewResult

MAX_PREVIEW_BYTES = 1_048_576
MAX_PREVIEW_ROWS = 100
InferredType = Literal["string", "integer", "float", "boolean", "date"]
_INTEGER = re.compile(r"^[+-]?\d+$")


def _failure(code: ErrorCode, message: str) -> DataBridgeError:
    return DataBridgeError(code, message)


def _read_prefix(connector: Connector, filename: str) -> tuple[str, bool]:
    with connector.read(filename) as source:
        data = source.read(MAX_PREVIEW_BYTES)
        at_limit = len(data) == MAX_PREVIEW_BYTES and bool(source.read(1))
    try:
        decoder = codecs.getincrementaldecoder("utf-8-sig")()
        return decoder.decode(data, final=not at_limit), at_limit
    except UnicodeDecodeError as exc:
        raise _failure(ErrorCode.MALFORMED_FILE, "Preview is not valid UTF-8") from exc


def _csv_rows(text: str, limit: int, at_limit: bool) -> list[dict[str, str]]:
    lines = text.splitlines(keepends=True)
    if at_limit and lines and not lines[-1].endswith(("\n", "\r")):
        lines.pop()  # A budget-cut line cannot be treated as a complete CSV record.
    try:
        reader = csv.reader(lines, strict=True)
        header = next(reader, None)
        if not header or any(not field for field in header) or len(set(header)) != len(header):
            raise _failure(ErrorCode.MALFORMED_FILE, "CSV must have a unique, nonempty header")
        rows: list[dict[str, str]] = []
        for values in reader:
            if len(values) != len(header):
                raise _failure(ErrorCode.MALFORMED_FILE, "CSV row does not match its header")
            rows.append(dict(zip(header, values, strict=True)))
            if len(rows) == limit:
                break
    except csv.Error as exc:
        code = ErrorCode.PREVIEW_LIMIT_EXCEEDED if at_limit else ErrorCode.MALFORMED_FILE
        raise _failure(code, "CSV preview cannot be parsed within the byte limit") from exc
    if at_limit and len(rows) < limit:
        raise _failure(ErrorCode.PREVIEW_LIMIT_EXCEEDED, "CSV preview exceeds the byte limit")
    return rows


def _json_rows(text: str, limit: int, at_limit: bool) -> list[dict[str, Any]]:
    def reject_constant(_value: str) -> None:
        raise _failure(ErrorCode.MALFORMED_FILE, "JSON contains a nonstandard numeric value")

    if not at_limit:
        try:
            document = json.loads(text, parse_constant=reject_constant)
        except json.JSONDecodeError as exc:
            raise _failure(ErrorCode.MALFORMED_FILE, "JSON file is malformed") from exc
        if not isinstance(document, list) or any(not isinstance(row, dict) for row in document):
            raise _failure(ErrorCode.MALFORMED_FILE, "JSON preview expects an array of objects")
        return document[:limit]

    decoder = json.JSONDecoder(parse_constant=reject_constant)
    position = 0

    def skip_space() -> None:
        nonlocal position
        while position < len(text) and text[position].isspace():
            position += 1

    def incomplete() -> DataBridgeError:
        code = ErrorCode.PREVIEW_LIMIT_EXCEEDED if at_limit else ErrorCode.MALFORMED_FILE
        return _failure(code, "JSON preview cannot be parsed within the byte limit")

    skip_space()
    if position >= len(text) or text[position] != "[":
        raise _failure(ErrorCode.MALFORMED_FILE, "JSON preview expects an array of objects")
    position += 1
    rows: list[dict[str, Any]] = []
    after_comma = False
    while True:
        skip_space()
        if position >= len(text):
            raise incomplete()
        if text[position] == "]":
            if after_comma:
                raise _failure(ErrorCode.MALFORMED_FILE, "JSON array has a trailing comma")
            position += 1
            skip_space()
            if position != len(text):
                raise _failure(ErrorCode.MALFORMED_FILE, "JSON has trailing content")
            return rows
        try:
            item, position = decoder.raw_decode(text, position)
        except json.JSONDecodeError as exc:
            raise incomplete() from exc
        if not isinstance(item, dict):
            raise _failure(ErrorCode.MALFORMED_FILE, "JSON array entries must be objects")
        rows.append(item)
        after_comma = False
        skip_space()
        if position >= len(text):
            if len(rows) == limit and at_limit:
                return rows
            raise incomplete()
        delimiter = text[position]
        if delimiter not in {",", "]"}:
            raise _failure(ErrorCode.MALFORMED_FILE, "JSON array entry has no valid delimiter")
        if len(rows) == limit:
            return rows
        if delimiter == "]":
            position += 1
            skip_space()
            if position != len(text):
                raise _failure(ErrorCode.MALFORMED_FILE, "JSON has trailing content")
            return rows
        position += 1
        after_comma = True


def _type_of(value: Any, *, csv_value: bool) -> InferredType | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float" if math.isfinite(value) else "string"
    if isinstance(value, str):
        clean = value.strip()
        if not clean:
            return None
        if csv_value:
            if clean.lower() in {"true", "false"}:
                return "boolean"
            if _INTEGER.fullmatch(clean):
                return "integer"
            try:
                number = float(clean)
                if math.isfinite(number):
                    return "float"
            except ValueError:
                pass
        try:
            date.fromisoformat(clean)
            return "date"
        except ValueError:
            return "string"
    return "string"


def _schema(
    rows: list[dict[str, Any]], header: list[str] | None = None, *, csv_values: bool = False
) -> dict[str, InferredType]:
    result: dict[str, InferredType] = {field: "string" for field in header or []}
    observed: dict[str, InferredType] = {}
    for row in rows:
        for field, value in row.items():
            kind = _type_of(value, csv_value=csv_values)
            if kind is None:
                result.setdefault(field, "string")
                continue
            previous = observed.get(field)
            if previous is None:
                observed[field] = kind
            elif previous != kind:
                observed[field] = "float" if {previous, kind} == {"integer", "float"} else "string"
            result[field] = observed[field]
    return result


def preview(connector: Connector, filename: str, limit: int) -> PreviewResult:
    validate_filename(filename)
    suffix = PurePath(filename).suffix.lower()
    if suffix not in {".csv", ".json"}:
        raise _failure(ErrorCode.UNSUPPORTED_PREVIEW_FORMAT, "Preview supports CSV and JSON files")
    if not 1 <= limit <= MAX_PREVIEW_ROWS:
        raise _failure(ErrorCode.INVALID_REQUEST, "limit must be between 1 and 100")
    text, at_limit = _read_prefix(connector, filename)
    if suffix == ".json":
        rows = _json_rows(text, limit, at_limit)
        return PreviewResult(filename=filename, format="json", rows=rows, schema=_schema(rows))
    rows = _csv_rows(text, limit, at_limit)
    header = next(csv.reader(io.StringIO(text)), [])
    return PreviewResult(
        filename=filename, format="csv", rows=rows, schema=_schema(rows, header, csv_values=True)
    )
