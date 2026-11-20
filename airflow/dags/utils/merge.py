"""Run dags/sql/merge/01_merge_raw_to_core.sql for one batch_id."""

from __future__ import annotations

import os
import re
from pathlib import Path

from services.snowflake_subscriber.db import connect

# airflow/dags/utils/merge.py -> repo root is parents[3]
DEFAULT_SQL = Path(__file__).resolve().parents[1] / 'sql' / 'merge' / '01_merge_raw_to_core.sql'


# Strip full-line SQL comments so semicolons inside comments do not split statements.
def _strip_line_comments(sql_text: str) -> str:
    lines: list[str] = []
    for line in sql_text.splitlines():
        if line.lstrip().startswith('--'):
            continue
        lines.append(line)
    return '\n'.join(lines)


# Split the merge script into one executable statement per piece.
def _statements(sql_text: str) -> list[str]:
    cleaned = _strip_line_comments(sql_text)
    cleaned = re.sub(r'(?im)^\s*BEGIN\s*;\s*$', '', cleaned)
    cleaned = re.sub(r'(?im)^\s*COMMIT\s*;\s*$', '', cleaned)
    parts: list[str] = []
    buf: list[str] = []
    for line in cleaned.splitlines():
        buf.append(line)
        if line.rstrip().endswith(';'):
            stmt = '\n'.join(buf).strip().rstrip(';').strip()
            buf = []
            if stmt:
                parts.append(stmt)
    tail = '\n'.join(buf).strip().rstrip(';').strip()
    if tail:
        parts.append(tail)
    return parts


# Run the RAW→CORE merge for one batch_id; returns the final COUNT(*) result rows.
def run_merge(batch_id: str, sql_path: Path | None = None) -> list[tuple]:
    path = sql_path or Path(os.environ.get('PO_MERGE_SQL', str(DEFAULT_SQL)))
    text = path.read_text()
    if not re.search(r"SET\s+batch_id\s*=", text, flags=re.I):
        raise RuntimeError(f'no SET batch_id in {path}')
    text = re.sub(
        r"SET\s+batch_id\s*=\s*'[^']*'\s*;",
        f"SET batch_id = '{batch_id}';",
        text,
        count=1,
        flags=re.I,
    )

    counts: list[tuple] = []
    conn = connect()
    try:
        conn.autocommit(False)
        cur = conn.cursor()
        try:
            for stmt in _statements(text):
                cur.execute(stmt)
                if stmt.lstrip().upper().startswith('SELECT'):
                    counts = list(cur.fetchall())
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()
    return counts
