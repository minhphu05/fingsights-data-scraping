from __future__ import annotations

import os

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, make_url


def create_tls_mysql_engine(
    database_url: str,
    ca_file: str,
    *,
    pool_size: int = 1,
    password_env: str | None = None,
) -> Engine:
    if not database_url:
        raise RuntimeError("A MySQL database URL is required")
    if not ca_file:
        raise RuntimeError("The Node-specific MySQL CA file path is required; refusing a plaintext connection")
    if password_env:
        password = os.getenv(password_env)
        if password:
            database_url = make_url(database_url).set(password=password).render_as_string(hide_password=False)
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=pool_size,
        max_overflow=0,
        connect_args={
            "connect_timeout": 10,
            "read_timeout": 30,
            "write_timeout": 30,
            "ssl_ca": ca_file,
            "ssl_verify_cert": True,
            # Current Oracle endpoint certificates use a shared CN without IP SANs;
            # the pinned endpoint CA is still validated for every TLS connection.
            "ssl_verify_identity": False,
        },
    )
    @event.listens_for(engine, "connect")
    def _use_utc_session(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SET time_zone = '+00:00'")
        finally:
            cursor.close()
    return engine

