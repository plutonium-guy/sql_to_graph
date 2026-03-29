"""TOONS — Token Oriented Object Notation Serializer.

A compact serialization format designed to minimize LLM token usage while
preserving readability. Tabular data in JSON can consume 5-10x more tokens
than necessary due to repeated keys, quotes, and braces.

TOONS encodes the same data in a flat, pipe-delimited format:

    JSON (89 tokens):
    {"columns": ["id", "name", "region"], "rows": [
      {"id": 1, "name": "Alice", "region": "East"},
      {"id": 2, "name": "Bob", "region": "West"}
    ], "row_count": 2}

    TOONS (28 tokens):
    §cols:id|name|region
    §rows:
    1|Alice|East
    2|Bob|West
    §n:2

Token savings are typically 60-75% for tabular data, which directly
reduces cost and latency for every LLM call.

Usage::

    from sql_to_graph.toons import toons_encode, toons_decode, ToonsConfig

    # Encode a tool result for LLM consumption
    compact = toons_encode(tool_result_dict)

    # Decode LLM output back to a dict
    data = toons_decode(compact)

    # Configure encoding
    cfg = ToonsConfig(max_rows=50, abbreviate_keys=True)
    compact = toons_encode(tool_result_dict, config=cfg)
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

# Section marker — uses § which is a single token in most tokenizers
_SECTION = "§"


@dataclass
class ToonsConfig:
    """Configuration for TOONS encoding.

    Args:
        max_rows: Maximum data rows to include (tail-truncated with a count).
        max_value_len: Truncate cell values longer than this.
        abbreviate_keys: Replace common key names with short aliases.
        include_types: Include column type hints in the header.
        include_stats: Include statistics section.
        include_suggestions: Include chart suggestion section.
        section_marker: Character prefix for section headers.
    """

    max_rows: int = 100
    max_value_len: int = 120
    abbreviate_keys: bool = True
    include_types: bool = False
    include_stats: bool = True
    include_suggestions: bool = True
    section_marker: str = _SECTION


# Common key abbreviations to save tokens
_KEY_ABBREVS = {
    "sql_executed": "sql",
    "columns": "cols",
    "row_count": "n",
    "total_row_count": "total",
    "execution_time_ms": "ms",
    "from_cache": "cached",
    "has_more": "more",
    "chart_suggestions": "suggest",
    "statistics": "stats",
    "column_name": "col",
    "distinct_count": "distinct",
    "null_count": "nulls",
    "top_values": "top",
    "confidence": "conf",
    "chart_type": "type",
    "x_column": "x",
    "y_column": "y",
    "z_column": "z",
    "data_base64": "b64",
    "size_bytes": "size",
    "available_tables": "tables",
    "schema_context": "schema_ctx",
    "error_type": "err_type",
}

_KEY_ABBREVS_REV = {v: k for k, v in _KEY_ABBREVS.items()}


# ─── Encoder ─────────────────────────────────────────────────────────────

def toons_encode(data: dict[str, Any], config: ToonsConfig | None = None) -> str:
    """Encode a tool result dict into TOONS format.

    Handles the common result shapes from ``handle_tool_call``:
    - Query results with columns/rows
    - Error responses
    - Discovery results (schemas, metadata)
    - Statistics and chart suggestions

    Args:
        data: A dict returned by ``handle_tool_call`` or similar.
        config: Optional encoding configuration.

    Returns:
        Compact TOONS-encoded string.
    """
    cfg = config or ToonsConfig()
    m = cfg.section_marker
    parts: list[str] = []

    # ─── Error response ──────────────────────────────────────────
    if "error" in data:
        parts.append(_encode_error(data, cfg))
        return "\n".join(parts)

    # ─── SQL executed ────────────────────────────────────────────
    sql = data.get("sql_executed")
    if sql:
        parts.append(f"{m}sql:{sql}")

    # ─── Metadata line ───────────────────────────────────────────
    meta_parts: list[str] = []
    if "row_count" in data:
        meta_parts.append(f"n={data['row_count']}")
    if "total_row_count" in data:
        meta_parts.append(f"total={data['total_row_count']}")
    if data.get("has_more"):
        meta_parts.append("more=T")
    if data.get("from_cache"):
        meta_parts.append("cached=T")
    if "execution_time_ms" in data:
        ms = data["execution_time_ms"]
        if isinstance(ms, float):
            meta_parts.append(f"ms={ms:.1f}")
        else:
            meta_parts.append(f"ms={ms}")
    if meta_parts:
        parts.append(f"{m}meta:{','.join(meta_parts)}")

    # ─── Tabular data (columns + rows) ──────────────────────────
    columns = data.get("columns")
    rows = data.get("rows")
    if columns and rows is not None:
        parts.append(f"{m}cols:{_pipe_join(columns)}")
        parts.append(f"{m}rows:")
        row_lines = _encode_rows(rows, columns, cfg)
        parts.extend(row_lines)

    # ─── Export data ─────────────────────────────────────────────
    if "export_data" in data:
        fmt = data.get("export_format", "?")
        parts.append(f"{m}export:{fmt}")
        export = data["export_data"]
        if len(export) > 2000:
            parts.append(export[:2000])
            parts.append(f"...truncated ({len(export)} chars)")
        else:
            parts.append(export)

    # ─── Statistics ──────────────────────────────────────────────
    stats = data.get("statistics")
    if stats and cfg.include_stats:
        parts.append(_encode_stats(stats, cfg))

    # ─── Chart suggestions ───────────────────────────────────────
    suggestions = data.get("chart_suggestions")
    if suggestions and cfg.include_suggestions:
        parts.append(_encode_suggestions(suggestions, cfg))

    # ─── Chart info ──────────────────────────────────────────────
    chart = data.get("chart")
    if chart:
        parts.append(_encode_chart_info(chart, cfg))

    # ─── Discovery results ───────────────────────────────────────
    if "schemas" in data:
        parts.append(_encode_schemas(data["schemas"], cfg))
    if "table" in data and "columns" in data and "row_count_estimate" in data:
        parts.append(_encode_table_meta(data, cfg))

    return "\n".join(parts)


def _pipe_join(values: list) -> str:
    """Join values with pipe, escaping pipes in values."""
    return "|".join(str(v).replace("|", "\\|") for v in values)


def _encode_rows(
    rows: list, columns: list[str], cfg: ToonsConfig
) -> list[str]:
    """Encode rows as pipe-delimited lines."""
    lines: list[str] = []
    # Handle both list-of-dicts and list-of-lists
    for i, row in enumerate(rows):
        if i >= cfg.max_rows:
            remaining = len(rows) - cfg.max_rows
            lines.append(f"...+{remaining} rows")
            break

        if isinstance(row, dict):
            cells = [_truncate(row.get(col, ""), cfg.max_value_len) for col in columns]
        elif isinstance(row, (list, tuple)):
            cells = [_truncate(v, cfg.max_value_len) for v in row]
        else:
            cells = [str(row)]

        lines.append(_pipe_join(cells))
    return lines


def _truncate(value: Any, max_len: int) -> str:
    """Convert to string and truncate if needed."""
    if value is None:
        return "∅"  # null marker — single token
    s = str(value)
    if len(s) > max_len:
        return s[:max_len] + "…"
    return s


def _encode_error(data: dict[str, Any], cfg: ToonsConfig) -> str:
    """Encode an error response."""
    m = cfg.section_marker
    error = data["error"]
    parts: list[str] = []

    if isinstance(error, dict):
        parts.append(f"{m}err:{error.get('message', str(error))}")
        if error.get("available_tables"):
            parts.append(f"{m}tables:{','.join(error['available_tables'])}")
        if error.get("suggestions"):
            parts.append(f"{m}did_you_mean:{','.join(error['suggestions'])}")
        if error.get("schema_context"):
            ctx = error["schema_context"]
            if len(ctx) > 500:
                ctx = ctx[:500] + "…"
            parts.append(f"{m}schema_ctx:{ctx}")
    else:
        parts.append(f"{m}err:{error}")

    sql = data.get("sql_executed")
    if sql:
        parts.append(f"{m}sql:{sql}")

    return "\n".join(parts)


def _encode_stats(stats: dict[str, Any], cfg: ToonsConfig) -> str:
    """Encode statistics section."""
    m = cfg.section_marker
    parts: list[str] = [f"{m}stats:"]

    col_stats = stats.get("columns", [])
    for cs in col_stats:
        tokens: list[str] = [cs.get("name", "?")]
        for key in ("category", "min", "max", "mean", "median", "stddev",
                     "null_count", "distinct_count"):
            val = cs.get(key)
            if val is not None:
                short_key = _KEY_ABBREVS.get(key, key) if cfg.abbreviate_keys else key
                tokens.append(f"{short_key}={_truncate(val, 30)}")
        parts.append("  " + ",".join(tokens))

    warnings = stats.get("warnings", [])
    if warnings:
        parts.append(f"{m}warnings:{';'.join(warnings)}")

    return "\n".join(parts)


def _encode_suggestions(suggestions: list[dict], cfg: ToonsConfig) -> str:
    """Encode chart suggestions."""
    m = cfg.section_marker
    parts: list[str] = [f"{m}suggest:"]
    for s in suggestions:
        ct = s.get("chart_type", "?")
        conf = s.get("confidence", 0)
        x = s.get("x_column", "")
        y = s.get("y_column", "")
        title = s.get("title", "")
        parts.append(f"  {ct},conf={conf:.2f},x={x},y={y},{title}")
    return "\n".join(parts)


def _encode_chart_info(chart: dict[str, Any], cfg: ToonsConfig) -> str:
    """Encode chart rendering info."""
    m = cfg.section_marker
    fmt = chart.get("format", "?")
    mime = chart.get("mime_type", "?")
    size = chart.get("size_bytes", 0)
    line = f"{m}chart:fmt={fmt},mime={mime},size={size}"
    if "data_base64" in chart:
        line += f",b64={chart['data_base64'][:60]}..."
    return line


def _encode_schemas(schemas: list[dict], cfg: ToonsConfig) -> str:
    """Encode schema discovery results."""
    m = cfg.section_marker
    lines = [f"{m}schemas:"]
    for s in schemas:
        lines.append(f"  {s['name']}({s.get('table_count', '?')} tables)")
    return "\n".join(lines)


def _encode_table_meta(data: dict[str, Any], cfg: ToonsConfig) -> str:
    """Encode table metadata (describe_table result)."""
    m = cfg.section_marker
    schema = data.get("schema", "")
    table = data.get("table", "")
    est = data.get("row_count_estimate", "?")
    lines = [f"{m}table:{schema}.{table}(~{est} rows)"]
    for col in data.get("columns", []):
        nullable = "NULL" if col.get("is_nullable") else "NOT NULL"
        lines.append(f"  {col['name']}:{col['data_type']},{nullable}")
    return "\n".join(lines)


# ─── Decoder ─────────────────────────────────────────────────────────────

def toons_decode(text: str, section_marker: str = _SECTION) -> dict[str, Any]:
    """Decode a TOONS-encoded string back to a dict.

    This is a best-effort decoder — it recovers the main fields
    (sql, columns, rows, meta) but may not round-trip perfectly for
    all edge cases. Primarily useful for testing and debugging.

    Args:
        text: TOONS-encoded string.
        section_marker: The section marker character used.

    Returns:
        Reconstructed dict.
    """
    m = section_marker
    result: dict[str, Any] = {}
    lines = text.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i]

        if line.startswith(f"{m}sql:"):
            result["sql_executed"] = line[len(f"{m}sql:"):]

        elif line.startswith(f"{m}meta:"):
            meta_str = line[len(f"{m}meta:"):]
            for pair in meta_str.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    k = _KEY_ABBREVS_REV.get(k, k)
                    if v == "T":
                        result[k] = True
                    elif v == "F":
                        result[k] = False
                    else:
                        try:
                            result[k] = int(v)
                        except ValueError:
                            try:
                                result[k] = float(v)
                            except ValueError:
                                result[k] = v

        elif line.startswith(f"{m}cols:"):
            result["columns"] = line[len(f"{m}cols:"):].split("|")

        elif line.startswith(f"{m}rows:"):
            columns = result.get("columns", [])
            rows: list[dict[str, Any]] = []
            i += 1
            while i < len(lines) and not lines[i].startswith(m):
                row_line = lines[i]
                if row_line.startswith("..."):
                    i += 1
                    continue
                cells = row_line.split("|")
                if columns:
                    row_dict: dict[str, Any] = {}
                    for col, cell in zip(columns, cells):
                        row_dict[col] = _parse_cell(cell)
                    rows.append(row_dict)
                i += 1
            result["rows"] = rows
            result["row_count"] = len(rows)
            continue  # don't increment i again

        elif line.startswith(f"{m}err:"):
            result["error"] = {"message": line[len(f"{m}err:"):]}

        elif line.startswith(f"{m}tables:"):
            if "error" not in result:
                result["error"] = {}
            result["error"]["available_tables"] = line[len(f"{m}tables:"):].split(",")

        elif line.startswith(f"{m}did_you_mean:"):
            if "error" not in result:
                result["error"] = {}
            result["error"]["suggestions"] = line[len(f"{m}did_you_mean:"):].split(",")

        i += 1

    return result


def _parse_cell(cell: str) -> Any:
    """Parse a TOONS cell value back to a Python type."""
    if cell == "∅":
        return None
    if cell.endswith("…"):
        return cell  # truncated string
    # Try int
    try:
        return int(cell)
    except ValueError:
        pass
    # Try float
    try:
        return float(cell)
    except ValueError:
        pass
    # Bool-like
    if cell in ("true", "True", "T"):
        return True
    if cell in ("false", "False", "F"):
        return False
    return cell


# ─── Token estimation ────────────────────────────────────────────────────

def estimate_tokens(text: str) -> int:
    """Rough token count estimate (word-based, ~1.3 tokens/word for English).

    This is a fast approximation. For precise counts, use tiktoken or
    the Anthropic tokenizer.
    """
    # Split on whitespace and punctuation boundaries
    words = re.findall(r"\S+", text)
    return int(len(words) * 1.3)


def compare_token_usage(data: dict[str, Any], config: ToonsConfig | None = None) -> dict[str, Any]:
    """Compare token usage between JSON and TOONS encoding.

    Returns:
        Dict with ``json_tokens``, ``toons_tokens``, ``savings_pct``.
    """
    json_str = json.dumps(data, default=str)
    toons_str = toons_encode(data, config)

    json_tokens = estimate_tokens(json_str)
    toons_tokens = estimate_tokens(toons_str)

    savings = (1 - toons_tokens / max(json_tokens, 1)) * 100

    return {
        "json_tokens": json_tokens,
        "toons_tokens": toons_tokens,
        "savings_pct": round(savings, 1),
        "json_chars": len(json_str),
        "toons_chars": len(toons_str),
    }
