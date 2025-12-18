"""SQLAlchemy engine and session factory for metadata store."""

from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from .config import get_settings


settings = get_settings()


def _build_engine():  # pragma: no cover - simple configuration helper
    url = settings.url
    engine_kwargs = {
        "echo": settings.echo,
        "future": True,
    }

    if url.startswith("sqlite"):
        engine_kwargs.update(
            {
                "connect_args": {"check_same_thread": False, "timeout": 30},
                "poolclass": StaticPool,
            }
        )
    else:
        engine_kwargs["pool_size"] = settings.pool_size
        engine_kwargs["max_overflow"] = settings.max_overflow
        engine_kwargs["pool_pre_ping"] = settings.pool_pre_ping

    return create_engine(url, **engine_kwargs)


engine = _build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)


@contextmanager
def session_scope() -> Session:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
