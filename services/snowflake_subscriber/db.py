"""Snowflake connection for the RAW subscriber.

Prefers key-pair auth when SNOWFLAKE_PRIVATE_KEY_PATH is set; otherwise password
(ACCOUNTADMIN on the trial is fine until a least-privilege user exists).
"""

from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def _private_key_bytes(path: str, passphrase: str | None) -> bytes:
    raw = Path(path).expanduser().read_bytes()
    password = passphrase.encode() if passphrase else None
    key = serialization.load_pem_private_key(raw, password=password, backend=default_backend())
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect():
    account = os.environ['SNOWFLAKE_ACCOUNT']
    user = os.environ['SNOWFLAKE_USER']
    kwargs: dict = {
        'account': account,
        'user': user,
        'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'PO_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'PURCHASE_ORDER_DB'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAW'),
    }
    key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    if key_path:
        kwargs['private_key'] = _private_key_bytes(
            key_path, os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
        )
    else:
        password = os.getenv('SNOWFLAKE_PASSWORD')
        if not password:
            raise RuntimeError('set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD')
        kwargs['password'] = password
    return snowflake.connector.connect(**kwargs)
