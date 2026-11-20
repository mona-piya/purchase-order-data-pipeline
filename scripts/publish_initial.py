#!/usr/bin/env python3
"""Publish the initial extract to Kafka so the target subscriber can be tested.

Usage (from repo root, venv active, Kafka up):
  python scripts/publish_initial.py
  python scripts/publish_initial.py --source-url http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

# Allow `python scripts/publish_initial.py` from the repo root.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.common.models import PurchaseOrderExtract  # noqa: E402
from services.common.publish import make_producer, publish_orders  # noqa: E402


# CLI entry: GET initial extract from source API and publish to Kafka.
def main() -> None:
    load_dotenv(ROOT / '.env')
    parser = argparse.ArgumentParser(description='Publish initial purchase orders to Kafka')
    parser.add_argument('--source-url', default=os.getenv('SOURCE_API_URL', 'http://127.0.0.1:8000'))
    parser.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092'))
    parser.add_argument('--batch-id', default=None, help='defaults to a new UUID')
    args = parser.parse_args()

    batch_id = args.batch_id or f'initial-{uuid.uuid4().hex[:8]}'
    url = f'{args.source_url.rstrip("/")}/purchase-orders?load_type=initial'
    response = httpx.get(url, timeout=60.0)
    response.raise_for_status()
    extract = PurchaseOrderExtract.model_validate(response.json())

    producer = make_producer(args.bootstrap)
    count = publish_orders(
        producer,
        extract.purchased_orders,
        batch_id=batch_id,
        load_type=extract.load_type,
        extracted_at=extract.extracted_at,
    )
    print(f'published {count} orders batch_id={batch_id} at {datetime.now(timezone.utc).isoformat()}')


if __name__ == '__main__':
    main()
