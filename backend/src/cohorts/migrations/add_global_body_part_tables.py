"""Idempotent migration: create global Body Part QC tables and migrate data.

Creates:
* ``body_part_global_sample`` — global labeled sample pool
* ``body_part_model`` — trained model registry
* Adds ``selected_model_id`` FK column to ``cohort_body_part_qc_state``

Auto-migration:
* Copies existing ``training_samples`` from cohort state into the global pool
* Registers the existing per-cohort classifier as a ``body_part_model`` row
  marked ``is_default=True``

Safe to call repeatedly; checks for table/column existence before DDL.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


def _table_exists(conn: Connection, table: str) -> bool:
    sql = text(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_name = :t LIMIT 1"
    )
    return conn.execute(sql, {"t": table}).first() is not None


def _has_column(conn: Connection, table: str, column: str) -> bool:
    sql = text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = :t AND column_name = :c LIMIT 1"
    )
    return conn.execute(sql, {"t": table, "c": column}).first() is not None


def _create_global_sample_table(conn: Connection) -> None:
    if _table_exists(conn, "body_part_global_sample"):
        return
    conn.execute(text("""
        CREATE TABLE body_part_global_sample (
            id SERIAL PRIMARY KEY,
            stack_id INTEGER NOT NULL,
            slice_index INTEGER NOT NULL,
            label VARCHAR(50) NOT NULL,
            orientation VARCHAR(20) NOT NULL DEFAULT 'unknown',
            source_cohort_name VARCHAR(200) NOT NULL,
            contributed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_global_sample_stack_slice UNIQUE (stack_id, slice_index)
        )
    """))
    logger.info("Created table body_part_global_sample")


def _create_model_table(conn: Connection) -> None:
    if _table_exists(conn, "body_part_model"):
        return
    conn.execute(text("""
        CREATE TABLE body_part_model (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            classes JSONB NOT NULL,
            label_remap JSONB NOT NULL DEFAULT '{}'::jsonb,
            artifact_path TEXT NOT NULL,
            artifact_sha256 VARCHAR(64) NOT NULL,
            meta JSONB NOT NULL DEFAULT '{}'::jsonb,
            accuracy DOUBLE PRECISION,
            n_samples INTEGER NOT NULL DEFAULT 0,
            trained_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_default BOOLEAN NOT NULL DEFAULT FALSE
        )
    """))
    logger.info("Created table body_part_model")


def _add_selected_model_id_column(conn: Connection) -> None:
    if not _table_exists(conn, "cohort_body_part_qc_state"):
        return
    if _has_column(conn, "cohort_body_part_qc_state", "selected_model_id"):
        return
    conn.execute(text("""
        ALTER TABLE cohort_body_part_qc_state
        ADD COLUMN selected_model_id INTEGER
            REFERENCES body_part_model(id) ON DELETE SET NULL
    """))
    logger.info("Added selected_model_id column to cohort_body_part_qc_state")


def _migrate_samples_to_global_pool(conn: Connection) -> int:
    """Copy training_samples from all cohort QC states into global pool."""
    if not _table_exists(conn, "cohort_body_part_qc_state"):
        return 0

    rows = conn.execute(text(
        "SELECT s.cohort_id, s.training_samples, c.name "
        "FROM cohort_body_part_qc_state s "
        "JOIN cohorts c ON c.id = s.cohort_id "
        "WHERE s.training_samples IS NOT NULL "
        "  AND jsonb_array_length(s.training_samples::jsonb) > 0"
    )).fetchall()

    n_inserted = 0
    for row in rows:
        cohort_name = row[2]
        samples = row[1]
        if isinstance(samples, str):
            samples = json.loads(samples)

        for s in samples:
            stack_id = int(s["stack_id"])
            slice_index = int(s["slice_index"])
            label = str(s["label"])
            orientation = str(s.get("orientation", "unknown"))
            contributed_at = s.get("approved_at") or datetime.now(timezone.utc).isoformat()

            # Upsert: update label if already exists (latest cohort wins)
            conn.execute(text("""
                INSERT INTO body_part_global_sample
                    (stack_id, slice_index, label, orientation,
                     source_cohort_name, contributed_at)
                VALUES (:sid, :idx, :lbl, :ori, :src, :at)
                ON CONFLICT (stack_id, slice_index)
                DO UPDATE SET
                    label = EXCLUDED.label,
                    orientation = EXCLUDED.orientation,
                    source_cohort_name = EXCLUDED.source_cohort_name,
                    contributed_at = EXCLUDED.contributed_at
            """), {
                "sid": stack_id, "idx": slice_index,
                "lbl": label, "ori": orientation,
                "src": cohort_name, "at": contributed_at,
            })
            n_inserted += 1

    if n_inserted:
        logger.info(
            "Migrated %d training samples into body_part_global_sample", n_inserted,
        )
    return n_inserted


def _register_existing_classifiers(conn: Connection) -> int:
    """Register existing per-cohort classifiers as body_part_model rows."""
    if not _table_exists(conn, "cohort_body_part_qc_state"):
        return 0

    # Check if we already have models registered (don't double-register)
    existing = conn.execute(text(
        "SELECT COUNT(*) FROM body_part_model"
    )).scalar()
    if existing and existing > 0:
        return 0

    rows = conn.execute(text(
        "SELECT s.cohort_id, s.classifier_path, s.classifier_sha256, "
        "       s.classifier_meta, s.categories, c.name "
        "FROM cohort_body_part_qc_state s "
        "JOIN cohorts c ON c.id = s.cohort_id "
        "WHERE s.classifier_path IS NOT NULL"
    )).fetchall()

    n_registered = 0
    for row in rows:
        cohort_id = row[0]
        classifier_path = row[1]
        classifier_sha256 = row[2]
        classifier_meta = row[3] or {}
        categories = row[4] or []
        cohort_name = row[5]

        if isinstance(classifier_meta, str):
            classifier_meta = json.loads(classifier_meta)
        if isinstance(categories, str):
            categories = json.loads(categories)

        classes = classifier_meta.get("classes", categories)
        accuracy = classifier_meta.get("accuracy")
        n_samples = classifier_meta.get("n_samples", 0)
        trained_at = classifier_meta.get("trained_at") or datetime.now(timezone.utc).isoformat()

        name = f"{len(classes)}-class ({'/'.join(classes)})"
        description = f"Auto-migrated from cohort '{cohort_name}'"

        result = conn.execute(text("""
            INSERT INTO body_part_model
                (name, description, classes, label_remap, artifact_path,
                 artifact_sha256, meta, accuracy, n_samples, trained_at, is_default)
            VALUES (:name, :desc, :classes, :remap, :path,
                    :sha, :meta, :acc, :ns, :at, :default)
            RETURNING id
        """), {
            "name": name,
            "desc": description,
            "classes": json.dumps(classes),
            "remap": json.dumps({}),
            "path": classifier_path,
            "sha": classifier_sha256 or "",
            "meta": json.dumps(classifier_meta),
            "acc": accuracy,
            "ns": n_samples,
            "at": trained_at,
            "default": True,  # First registered model is default
        })
        model_id = result.scalar()

        # Link the cohort to its migrated model
        conn.execute(text("""
            UPDATE cohort_body_part_qc_state
            SET selected_model_id = :mid
            WHERE cohort_id = :cid
        """), {"mid": model_id, "cid": cohort_id})

        n_registered += 1
        logger.info(
            "Registered classifier from cohort '%s' as model id=%d (%s)",
            cohort_name, model_id, name,
        )

    return n_registered


def ensure_migrated() -> None:
    """Run the full migration (idempotent)."""
    from db.session import engine

    with engine.begin() as conn:
        _create_model_table(conn)
        _create_global_sample_table(conn)
        _add_selected_model_id_column(conn)
        _migrate_samples_to_global_pool(conn)
        _register_existing_classifiers(conn)


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    ensure_migrated()
