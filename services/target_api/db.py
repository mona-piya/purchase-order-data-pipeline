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


# Open SQLite and ensure the purchase_orders table exists.
def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute(SCHEMA)
    conn.commit()
    return conn


# Return one stored order as a dict, or None.
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


# List all orders, or only those with this batch_id.
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


# Insert or replace when incoming LastUpdated is not older than stored.
# Returns 'created', 'updated', or 'rejected'.
def upsert(
    conn: sqlite3.Connection,
    *,
    order_number: str,
    last_updated: datetime,
    batch_id: str,
    document: dict,
) -> str:
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


# Total number of rows in purchase_orders.
def count_orders(conn: sqlite3.Connection) -> int:
    return int(conn.execute('SELECT COUNT(*) FROM purchase_orders').fetchone()[0])


# How many rows were written with this batch_id.
def count_batch(conn: sqlite3.Connection, batch_id: str) -> int:
    return int(
        conn.execute(
            'SELECT COUNT(*) FROM purchase_orders WHERE batch_id = ?', (batch_id,),
        ).fetchone()[0]
    )
