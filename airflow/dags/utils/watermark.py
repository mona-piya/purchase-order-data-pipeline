"""Read/write CORE.PIPELINE_WATERMARK (one control row per load_type)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from services.snowflake_subscriber.db import connect

log = logging.getLogger(__name__)

PIPELINE_NAME = 'purchase_order_pipeline'

# Prefer incremental cursor; fall back to initial after bootstrap before first incremental.
_SELECT = """
SELECT changed_since, load_type
FROM CORE.PIPELINE_WATERMARK
WHERE pipeline_name = %s
  AND load_type = %s
  AND changed_since IS NOT NULL
"""

_MERGE = """
MERGE INTO CORE.PIPELINE_WATERMARK AS t
USING (
    SELECT
        %s AS pipeline_name,
        %s AS load_type,
        TO_TIMESTAMP_TZ(%s) AS changed_since,
        %s AS batch_id
) AS s
ON t.pipeline_name = s.pipeline_name
   AND t.load_type = s.load_type
WHEN MATCHED THEN UPDATE SET
    changed_since = s.changed_since,
    load_dts = CURRENT_TIMESTAMP(),
    batch_id = s.batch_id
WHEN NOT MATCHED THEN INSERT (
    pipeline_name, load_type, changed_since, load_dts, batch_id
) VALUES (
    s.pipeline_name, s.load_type, s.changed_since, CURRENT_TIMESTAMP(), s.batch_id
)
"""


def _to_api_ts(value: Any) -> str | None:
    """Format a Snowflake / Python timestamp as ISO-8601 with Z for the Source API."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace('+00:00', 'Z') if value.endswith('+00:00') else value
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    return str(value)


def get_changed_since(pipeline_name: str = PIPELINE_NAME) -> str | None:
    """Cutoff for incremental extract: incremental slot, else initial slot."""
    conn = connect()
    try:
        cur = conn.cursor()
        for slot in ('incremental', 'initial'):
            cur.execute(_SELECT, (pipeline_name, slot))
            row = cur.fetchone()
            if row and row[0] is not None:
                ts = _to_api_ts(row[0])
                log.info(
                    'PIPELINE_WATERMARK changed_since=%s pipeline=%s from_slot=%s',
                    ts, pipeline_name, slot,
                )
                cur.close()
                return ts
        cur.close()
    finally:
        conn.close()
    log.info('PIPELINE_WATERMARK changed_since unset pipeline=%s', pipeline_name)
    return None


def set_watermark(
    *,
    load_type: str,
    changed_since: str,
    batch_id: str,
    pipeline_name: str = PIPELINE_NAME,
) -> None:
    """Upsert the control row for this load_type after a successful verify."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(
            _MERGE,
            (pipeline_name, load_type, changed_since, batch_id),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    log.info(
        'PIPELINE_WATERMARK upserted pipeline=%s load_type=%s changed_since=%s batch_id=%s',
        pipeline_name, load_type, changed_since, batch_id,
    )
