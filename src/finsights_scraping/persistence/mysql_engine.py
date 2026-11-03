from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine


def create_tls_mysql_engine(database_url: str, ca_file: str, *, pool_size: int = 1) -> Engine:
    if not database_url:
        raise RuntimeError("A MySQL database URL is required")
    if not ca_file:
        raise RuntimeError("The Node-specific MySQL CA file path is required; refusing a plaintext connection")
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=pool_size,
        max_overflow=0,
        connect_args={
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

