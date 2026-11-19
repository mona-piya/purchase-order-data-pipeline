"""SQLite store for the target API. One writer is enough for this demo."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from services.common.models import parse_timestamp

SCHEMA = """
CREATE TABLE IF NOT EXISTS purchase_orders (
    order_number TEXT PRIMARY KEY,
    last_updated TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    document TEXT NOT NULL
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(SCHEMA)
    conn.commit()
    return conn


def get_order(conn: sqlite3.Connection, order_number: str) -> dict | None:
    row = conn.execute(
        'SELECT order_number, last_updated, batch_id, document FROM purchase_orders WHERE order_number = ?',
        (order_number,),
    ).fetchone()
    if row is None:
        return None
    return {
        'order_number': row['order_number'],
        'last_updated': row['last_updated'],
        'batch_id': row['batch_id'],
        'order': json.loads(row['document']),
    }


def list_orders(conn: sqlite3.Connection, batch_id: str | None = None) -> list[dict]:
    if batch_id is None:
        rows = conn.execute(
            'SELECT order_number, last_updated, batch_id, document '
            'FROM purchase_orders ORDER BY order_number'
        ).fetchall()
    else:
        rows = conn.execute(
            'SELECT order_number, last_updated, batch_id, document '
            'FROM purchase_orders WHERE batch_id = ? ORDER BY order_number',
            (batch_id,),
        ).fetchall()
    return [
        {
            'order_number': row['order_number'],
            'last_updated': row['last_updated'],
            'batch_id': row['batch_id'],
            'order': json.loads(row['document']),
        }
        for row in rows
    ]


def upsert(
    conn: sqlite3.Connection,
    *,
    order_number: str,
    last_updated: datetime,
    batch_id: str,
    document: dict,
) -> str:
    """Insert or replace when the incoming LastUpdated is not older.

    Returns 'created', 'updated', or 'rejected'.
    """
    existing = get_order(conn, order_number)
    incoming = last_updated
    if existing is not None:
        stored = parse_timestamp(existing['last_updated'])
        if incoming < stored:
            return 'rejected'
    conn.execute(
        """
        INSERT INTO purchase_orders (order_number, last_updated, batch_id, document)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(order_number) DO UPDATE SET
            last_updated = excluded.last_updated,
            batch_id = excluded.batch_id,
            document = excluded.document
        """,
        (order_number, incoming.isoformat().replace('+00:00', 'Z'), batch_id, json.dumps(document)),
    )
    conn.commit()
    return 'created' if existing is None else 'updated'


def count_orders(conn: sqlite3.Connection) -> int:
    return int(conn.execute('SELECT COUNT(*) FROM purchase_orders').fetchone()[0])


def count_batch(conn: sqlite3.Connection, batch_id: str) -> int:
    return int(
        conn.execute(
            'SELECT COUNT(*) FROM purchase_orders WHERE batch_id = ?', (batch_id,),
        ).fetchone()[0]
    )
