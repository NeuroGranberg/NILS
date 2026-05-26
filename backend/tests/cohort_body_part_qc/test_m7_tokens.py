"""M7 — metadata-DB token writer tests.

Uses a synthetic in-memory SQLite ``series_classification_cache`` table
(matching just the columns we touch) so we can exercise the writer
without wiring a real metadata DB.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from sqlalchemy import (
    Column, Integer, MetaData, String, Table, Text, create_engine, select,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from qc.body_part.tokens import (
    LOW_CONF_REVIEW_TOKEN,
    apply_picks_to_metadata,
    set_low_conf_tokens,
    wipe_low_conf_tokens_for_cohort,
    write_body_part_labels,
)


# ---------------------------------------------------------------------------
# In-memory metadata DB shim
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_meta_db():
    metadata = MetaData()
    scc = Table(
        "series_classification_cache", metadata,
        Column("series_stack_id", Integer, primary_key=True),
        Column("body_part", String(64), nullable=True),
        Column("manual_review_reasons_csv", Text, nullable=True),
        Column("dicom_origin_cohort", String(200), nullable=True),
    )

    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    metadata.create_all(engine)
    SessionLocal = sessionmaker(
        bind=engine, autoflush=False, autocommit=False,
        expire_on_commit=False, future=True,
    )

    # SQLite doesn't support PostgreSQL ANY(:ids) — patch the writer's
    # SQL by routing it through a custom session_factory and rewriting
    # to IN (...). The simplest approach: write rows and run our own
    # UPDATE / SELECT through SQLAlchemy directly, but the writer uses
    # raw SQL with ANY(:ids). Easiest: install an event listener that
    # rewrites the statement. Instead we wrap the writer's text() calls
    # by providing a session whose ``execute`` rewrites the SQL.
    class RewritingSession:
        def __init__(self, real):
            self._real = real
        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "ANY(:ids)" in sql:
                ids = params.get("ids", []) if params else []
                if not ids:
                    sql = sql.replace("= ANY(:ids)", "IN (NULL)")
                else:
                    placeholders = ",".join(str(int(i)) for i in ids)
                    sql = sql.replace("= ANY(:ids)", f"IN ({placeholders})")
                from sqlalchemy import text as _text
                stmt = _text(sql)
                if params:
                    params = {k: v for k, v in params.items() if k != "ids"}
            if "POSITION(:tok IN" in sql:
                # SQLite uses INSTR; rewrite to instr(...) > 0.
                sql = sql.replace(
                    "POSITION(:tok IN COALESCE(manual_review_reasons_csv, ''))",
                    "INSTR(COALESCE(manual_review_reasons_csv, ''), :tok)",
                )
                from sqlalchemy import text as _text
                stmt = _text(sql)
            return self._real.execute(stmt, params or {})
        def commit(self): self._real.commit()
        def rollback(self): self._real.rollback()
        def close(self): self._real.close()
        def __enter__(self): return self
        def __exit__(self, *a): self.close()

    def factory():
        return RewritingSession(SessionLocal())

    yield {"engine": engine, "scc": scc, "factory": factory,
           "SessionLocal": SessionLocal}


def _seed(session_factory, rows):
    """Insert raw rows into the fake table."""
    from sqlalchemy import text
    sql = text(
        "INSERT INTO series_classification_cache "
        "(series_stack_id, body_part, manual_review_reasons_csv, "
        " dicom_origin_cohort) "
        "VALUES (:sid, :bp, :csv, :cohort)"
    )
    with session_factory() as db:
        for r in rows:
            db.execute(sql, {
                "sid": r["sid"], "bp": r.get("bp"),
                "csv": r.get("csv"), "cohort": r.get("cohort", "alpha"),
            })
        db.commit()


def _read_rows(SessionLocal):
    from sqlalchemy import text
    with SessionLocal() as db:
        return db.execute(text(
            "SELECT series_stack_id, body_part, manual_review_reasons_csv "
            "FROM series_classification_cache ORDER BY series_stack_id"
        )).fetchall()


# ---------------------------------------------------------------------------
# write_body_part_labels
# ---------------------------------------------------------------------------


def test_write_body_part_labels_updates_only_changed(fake_meta_db):
    _seed(fake_meta_db["factory"], [
        {"sid": 1, "bp": "brain"},
        {"sid": 2, "bp": None},
        {"sid": 3, "bp": "spine"},
    ])
    n = write_body_part_labels(
        [(1, "Brain"), (2, "Brain"), (3, "spine")],  # 3 unchanged
        session_factory=fake_meta_db["factory"],
    )
    assert n == 2  # only 1 and 2 actually changed
    rows = _read_rows(fake_meta_db["SessionLocal"])
    assert rows[0].body_part == "Brain"
    assert rows[1].body_part == "Brain"
    assert rows[2].body_part == "spine"


def test_write_body_part_labels_empty(fake_meta_db):
    assert write_body_part_labels([], session_factory=fake_meta_db["factory"]) == 0


def test_write_body_part_labels_clears_to_none(fake_meta_db):
    _seed(fake_meta_db["factory"], [{"sid": 1, "bp": "brain"}])
    n = write_body_part_labels(
        [(1, None)], session_factory=fake_meta_db["factory"],
    )
    assert n == 1
    rows = _read_rows(fake_meta_db["SessionLocal"])
    assert rows[0].body_part is None


# ---------------------------------------------------------------------------
# set_low_conf_tokens
# ---------------------------------------------------------------------------


def test_set_low_conf_tokens_add_and_remove(fake_meta_db):
    _seed(fake_meta_db["factory"], [
        {"sid": 1, "csv": None},
        {"sid": 2, "csv": "other:reason"},
        {"sid": 3, "csv": f"{LOW_CONF_REVIEW_TOKEN}"},
    ])
    n_added, n_removed = set_low_conf_tokens(
        add_ids=[1, 2], remove_ids=[3],
        session_factory=fake_meta_db["factory"],
    )
    assert (n_added, n_removed) == (2, 1)
    rows = _read_rows(fake_meta_db["SessionLocal"])
    assert LOW_CONF_REVIEW_TOKEN in rows[0].manual_review_reasons_csv
    assert "other:reason" in rows[1].manual_review_reasons_csv
    assert LOW_CONF_REVIEW_TOKEN in rows[1].manual_review_reasons_csv
    assert rows[2].manual_review_reasons_csv in (None, "")


def test_set_low_conf_tokens_idempotent(fake_meta_db):
    _seed(fake_meta_db["factory"], [
        {"sid": 1, "csv": LOW_CONF_REVIEW_TOKEN},
    ])
    n_added, n_removed = set_low_conf_tokens(
        add_ids=[1], remove_ids=[],
        session_factory=fake_meta_db["factory"],
    )
    assert (n_added, n_removed) == (0, 0)


def test_set_low_conf_tokens_empty(fake_meta_db):
    assert set_low_conf_tokens(
        [], [], session_factory=fake_meta_db["factory"],
    ) == (0, 0)


# ---------------------------------------------------------------------------
# wipe_low_conf_tokens_for_cohort
# ---------------------------------------------------------------------------


def test_wipe_low_conf_tokens_for_cohort(fake_meta_db):
    _seed(fake_meta_db["factory"], [
        {"sid": 1, "csv": LOW_CONF_REVIEW_TOKEN, "cohort": "alpha"},
        {"sid": 2, "csv": f"x,{LOW_CONF_REVIEW_TOKEN}", "cohort": "alpha"},
        {"sid": 3, "csv": LOW_CONF_REVIEW_TOKEN, "cohort": "beta"},
        {"sid": 4, "csv": "y", "cohort": "alpha"},
    ])
    n = wipe_low_conf_tokens_for_cohort(
        "alpha", session_factory=fake_meta_db["factory"],
    )
    assert n == 2
    rows = _read_rows(fake_meta_db["SessionLocal"])
    by_id = {r.series_stack_id: r for r in rows}
    assert LOW_CONF_REVIEW_TOKEN not in (by_id[1].manual_review_reasons_csv or "")
    assert LOW_CONF_REVIEW_TOKEN not in (by_id[2].manual_review_reasons_csv or "")
    # beta cohort untouched.
    assert LOW_CONF_REVIEW_TOKEN in (by_id[3].manual_review_reasons_csv or "")
    # row 4 had no token → unchanged.
    assert by_id[4].manual_review_reasons_csv == "y"


# ---------------------------------------------------------------------------
# apply_picks_to_metadata
# ---------------------------------------------------------------------------


def test_apply_picks_writes_labels_and_low_conf(fake_meta_db):
    _seed(fake_meta_db["factory"], [
        {"sid": 1, "bp": "brain", "csv": None},
        {"sid": 2, "bp": None, "csv": None},
        {"sid": 3, "bp": "spine", "csv": LOW_CONF_REVIEW_TOKEN},
    ])
    picks = [
        {"study_id": 1, "subject_code": "s1", "stacks": [
            {"stack_id": 1, "label": "Brain", "needs_check": False, "is_override": False},
            {"stack_id": 2, "label": "Brain-Neck", "needs_check": True, "is_override": False},
            {"stack_id": 3, "label": "Brain", "needs_check": False, "is_override": True},
        ]}
    ]
    stats = apply_picks_to_metadata(
        picks, session_factory=fake_meta_db["factory"],
    )
    assert stats["body_part_updated"] == 3
    assert stats["low_conf_added"] == 1
    assert stats["low_conf_removed"] == 1

    rows = _read_rows(fake_meta_db["SessionLocal"])
    by_id = {r.series_stack_id: r for r in rows}
    assert by_id[1].body_part == "Brain"
    assert by_id[2].body_part == "Brain-Neck"
    assert LOW_CONF_REVIEW_TOKEN in by_id[2].manual_review_reasons_csv
    # Override removes the low-conf flag.
    assert by_id[3].body_part == "Brain"
    assert LOW_CONF_REVIEW_TOKEN not in (by_id[3].manual_review_reasons_csv or "")


def test_apply_picks_skips_stacks_without_label(fake_meta_db):
    _seed(fake_meta_db["factory"], [
        {"sid": 1, "bp": "spine", "csv": None},
    ])
    picks = [
        {"study_id": 1, "stacks": [
            {"stack_id": 1, "label": None, "needs_check": True, "is_override": False},
        ]}
    ]
    stats = apply_picks_to_metadata(
        picks, session_factory=fake_meta_db["factory"],
    )
    assert stats["body_part_updated"] == 0
    rows = _read_rows(fake_meta_db["SessionLocal"])
    assert rows[0].body_part == "spine"  # unchanged
