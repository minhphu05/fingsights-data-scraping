from collections.abc import Generator
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session
import structlog
from src.config.settings import settings

logger = structlog.get_logger(__name__)

Base = declarative_base()


def get_database_url() -> str:
    """Build SQLAlchemy MySQL connection URL."""
    password = settings.mysql_password
    # URL encode password if needed
    from urllib.parse import quote_plus
    encoded_password = quote_plus(password)
    return (
        f"mysql+pymysql://{settings.mysql_user}:{encoded_password}@"
        f"{settings.mysql_host}:{settings.mysql_port}/{settings.mysql_database}"
        f"?charset=utf8mb4"
    )


engine = create_engine(
    get_database_url(),
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """Context manager for safe database transactions."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as exc:
        session.rollback()
        logger.error("Database transaction rolled back", error=str(exc))
        raise
    finally:
        session.close()
