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
DEFAULT_PREVIEW_ROWS = 5
MAX_PREVIEW_ROWS = 100
MAX_JSON_DEPTH = 32
_TOKEN_TAIL = 6  # the longest cut token that can end a budget: a "\uXXXX" escape
InferredType = Literal["string", "integer", "float", "boolean", "date"]
_INTEGER = re.compile(r"[+-]?[0-9]+")
_FLOAT = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")

# One field may fill the whole preview budget; the csv module's default limit is 128 KiB.
csv.field_size_limit(max(csv.field_size_limit(), MAX_PREVIEW_BYTES))


def preview(connector: Connector, filename: str, limit: int) -> PreviewResult:
    """Parse at most MAX_PREVIEW_BYTES of a file; the API layer validates `limit`."""
    validate_filename(filename)
    suffix = PurePath(filename).suffix.lower()
    if suffix not in {".csv", ".json"}:
        raise DataBridgeError(
            ErrorCode.UNSUPPORTED_PREVIEW_FORMAT, "Preview supports CSV and JSON files"
        )
    text, at_limit = _read_prefix(connector, filename)
    if suffix == ".json":
        rows = _json_rows(text, limit, at_limit)
        return PreviewResult(filename=filename, format="json", rows=rows, schema=_schema(rows))
    header, csv_rows = _csv_rows(text, limit, at_limit)
    return PreviewResult(
        filename=filename,
        format="csv",
        rows=csv_rows,
        schema=_schema(csv_rows, header, csv_values=True),
    )


def _read_prefix(connector: Connector, filename: str) -> tuple[str, bool]:
    """Read one byte past the budget, through short reads, to learn whether the file is longer."""
    data = bytearray()
    with connector.read(filename, limit=MAX_PREVIEW_BYTES + 1) as source:
        while len(data) <= MAX_PREVIEW_BYTES:
            chunk = source.read(MAX_PREVIEW_BYTES + 1 - len(data))
            if not chunk:
                break
            data += chunk
    at_limit = len(data) > MAX_PREVIEW_BYTES
    del data[MAX_PREVIEW_BYTES:]
    try:
        # A character cut at the budget is held back rather than reported as invalid.
        text = codecs.getincrementaldecoder("utf-8-sig")().decode(bytes(data), final=not at_limit)
    except UnicodeDecodeError as exc:
        raise DataBridgeError(ErrorCode.MALFORMED_FILE, "The file is not valid UTF-8 text") from exc
    return text, at_limit


def _csv_rows(text: str, limit: int, at_limit: bool) -> tuple[list[str], list[dict[str, str]]]:
    if at_limit:
        # The last record may continue past the budget, so parse only through the last line end.
        text = text[: max(text.rfind("\n"), text.rfind("\r")) + 1]
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    rows: list[dict[str, str]] = []
    try:
        header = next(reader, None)
        if header is None and at_limit:
            raise DataBridgeError(
                ErrorCode.PREVIEW_LIMIT_EXCEEDED, "The CSV header exceeds the preview byte limit"
            )
        if not header or any(not field for field in header) or len(set(header)) != len(header):
            raise DataBridgeError(
                ErrorCode.MALFORMED_FILE, "CSV must have a unique, nonempty header"
            )
        for values in reader:
            if not values:
                continue  # A blank line is not a record, as in csv.DictReader.
            if len(values) != len(header):
                raise DataBridgeError(
                    ErrorCode.MALFORMED_FILE, "A CSV row does not match its header"
                )
            rows.append(dict(zip(header, values, strict=True)))
            if len(rows) == limit:
                break
    except csv.Error as exc:
        code = ErrorCode.PREVIEW_LIMIT_EXCEEDED if at_limit else ErrorCode.MALFORMED_FILE
        raise DataBridgeError(
            code, "The CSV preview cannot be parsed within the byte limit"
        ) from exc
    if at_limit and len(rows) < limit:
        raise DataBridgeError(
            ErrorCode.PREVIEW_LIMIT_EXCEEDED, "The CSV preview exceeds the byte limit"
        )
    return header, rows


def _reject_constant(_name: str) -> None:
    raise DataBridgeError(ErrorCode.MALFORMED_FILE, "JSON contains a nonstandard numeric value")


def _finite_float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise DataBridgeError(ErrorCode.MALFORMED_FILE, "JSON contains a number out of range")
    return value


def _bounded_int(text: str) -> int:
    try:
        return int(text)
    except ValueError as exc:  # more digits than int() converts
        raise DataBridgeError(
            ErrorCode.MALFORMED_FILE, "JSON contains an integer with too many digits"
        ) from exc


_DECODER = json.JSONDecoder(
    parse_float=_finite_float, parse_int=_bounded_int, parse_constant=_reject_constant
)


def _json_rows(text: str, limit: int, at_limit: bool) -> list[dict[str, Any]]:
    try:
        rows = _json_prefix(text, limit) if at_limit else _json_document(text, limit)
    except RecursionError as exc:
        raise _too_deep() from exc
    for row in rows:
        _check_row(row)
    return rows


def _json_document(text: str, limit: int) -> list[dict[str, Any]]:
    """Validate a file that fits the budget in full, then return its first rows."""
    try:
        document = _DECODER.decode(text)
    except json.JSONDecodeError as exc:
        raise DataBridgeError(ErrorCode.MALFORMED_FILE, "The JSON file is malformed") from exc
    if not isinstance(document, list) or any(not isinstance(row, dict) for row in document):
        raise DataBridgeError(ErrorCode.MALFORMED_FILE, "JSON preview expects an array of objects")
    return document[:limit]


def _json_prefix(text: str, limit: int) -> list[dict[str, Any]]:
    """Parse array entries from a budget-cut prefix until `limit` rows are complete."""
    position = _skip_space(text, 0)
    if position >= len(text) or text[position] != "[":
        raise DataBridgeError(ErrorCode.MALFORMED_FILE, "JSON preview expects an array of objects")
    position += 1
    rows: list[dict[str, Any]] = []
    after_comma = False
    while True:
        position = _skip_space(text, position)
        if position >= len(text):
            raise _incomplete()
        if text[position] == "]":
            if after_comma:
                raise DataBridgeError(ErrorCode.MALFORMED_FILE, "JSON array has a trailing comma")
            return _end_of_array(text, position, rows)
        try:
            item, position = _DECODER.raw_decode(text, position)
        except json.JSONDecodeError as exc:
            if _cut_by_budget(exc, text):
                raise _incomplete() from exc
            raise DataBridgeError(ErrorCode.MALFORMED_FILE, "The JSON file is malformed") from exc
        if not isinstance(item, dict):
            raise DataBridgeError(ErrorCode.MALFORMED_FILE, "JSON array entries must be objects")
        rows.append(item)
        after_comma = False
        position = _skip_space(text, position)
        if position >= len(text):
            if len(rows) == limit:
                return rows
            raise _incomplete()
        delimiter = text[position]
        if delimiter not in {",", "]"}:
            raise DataBridgeError(
                ErrorCode.MALFORMED_FILE, "A JSON array entry has no valid delimiter"
            )
        if len(rows) == limit:
            return rows
        if delimiter == "]":
            return _end_of_array(text, position, rows)
        position += 1
        after_comma = True


def _cut_by_budget(error: json.JSONDecodeError, text: str) -> bool:
    """Whether more bytes could still complete the entry. The decoder reports an unterminated
    string at its start and a cut number or escape a few characters before the end; any other
    error proves the entry malformed whatever follows."""
    return error.msg.startswith("Unterminated string") or error.pos >= len(text) - _TOKEN_TAIL


def _skip_space(text: str, position: int) -> int:
    while position < len(text) and text[position] in " \t\n\r":
        position += 1
    return position


def _end_of_array(text: str, position: int, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if _skip_space(text, position + 1) != len(text):
        raise DataBridgeError(ErrorCode.MALFORMED_FILE, "JSON has trailing content")
    return rows


def _incomplete() -> DataBridgeError:
    return DataBridgeError(
        ErrorCode.PREVIEW_LIMIT_EXCEEDED, "The JSON preview cannot be parsed within the byte limit"
    )


def _too_deep() -> DataBridgeError:
    return DataBridgeError(
        ErrorCode.PREVIEW_LIMIT_EXCEEDED,
        f"JSON nesting is deeper than the preview limit of {MAX_JSON_DEPTH} levels",
    )


def _check_row(row: dict[str, Any]) -> None:
    """Bound nesting and reject unpaired surrogates, which no JSON response can encode."""
    pending: list[tuple[dict[str, Any] | list[Any], int]] = [(row, 1)]
    while pending:
        container, depth = pending.pop()
        if depth > MAX_JSON_DEPTH:
            raise _too_deep()
        pairs = container.items() if isinstance(container, dict) else enumerate(container)
        for key, value in pairs:
            for text in (key, value):
                if isinstance(text, str) and not _encodable(text):
                    raise DataBridgeError(
                        ErrorCode.MALFORMED_FILE, "JSON contains an unpaired surrogate escape"
                    )
            if isinstance(value, dict | list):
                pending.append((value, depth + 1))


def _encodable(text: str) -> bool:
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _type_of(value: Any, *, csv_value: bool) -> InferredType | None:
    """Infer with ASCII grammars; parsing already rejected non-finite JSON numbers."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if not isinstance(value, str):
        return "string"
    clean = value.strip()
    if not clean:
        return None
    if csv_value:
        if clean.lower() in {"true", "false"}:
            return "boolean"
        if _INTEGER.fullmatch(clean):
            return "integer"
        if _FLOAT.fullmatch(clean) and math.isfinite(float(clean)):
            return "float"
    if _DATE.fullmatch(clean):
        try:
            date.fromisoformat(clean)
        except ValueError:
            return "string"
        return "date"
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
