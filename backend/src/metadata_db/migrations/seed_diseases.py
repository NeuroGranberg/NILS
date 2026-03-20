"""
Migration: Seed diseases table

Inserts default disease records into the diseases table.
Handles both fresh databases and restored old backups.
Runs automatically on server startup via lifecycle.py.
"""

from __future__ import annotations

import logging
import time

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)

DISEASE_TYPES_SEED = [
    # MS subtypes (disease_id=2)
    {"disease_type_id": 1,  "disease_id": 2, "type_name": "CIS",  "description": "Clinically Isolated Syndrome", "sort_order": 1},
    {"disease_type_id": 2,  "disease_id": 2, "type_name": "RRMS", "description": "Relapsing-Remitting Multiple Sclerosis", "sort_order": 2},
    {"disease_type_id": 3,  "disease_id": 2, "type_name": "SPMS", "description": "Secondary Progressive Multiple Sclerosis", "sort_order": 3},
    {"disease_type_id": 4,  "disease_id": 2, "type_name": "PPMS", "description": "Primary Progressive Multiple Sclerosis", "sort_order": 4},
    {"disease_type_id": 5,  "disease_id": 2, "type_name": "PRMS", "description": "Progressive-Relapsing Multiple Sclerosis", "sort_order": 5},
    # ALS subtypes (disease_id=3)
    {"disease_type_id": 6,  "disease_id": 3, "type_name": "Limb-onset",   "description": "Spinal ALS — weakness begins in limbs", "sort_order": 1},
    {"disease_type_id": 7,  "disease_id": 3, "type_name": "Bulbar-onset", "description": "Bulbar ALS — symptoms begin in speech/swallowing muscles", "sort_order": 2},
    {"disease_type_id": 8,  "disease_id": 3, "type_name": "Familial",     "description": "Familial ALS — inherited genetic mutations (SOD1, C9ORF72)", "sort_order": 3},
    {"disease_type_id": 9,  "disease_id": 3, "type_name": "Sporadic",     "description": "Sporadic ALS — no known family history", "sort_order": 4},
    # Parkinson subtypes (disease_id=4)
    {"disease_type_id": 10, "disease_id": 4, "type_name": "Tremor-Dominant", "description": "Tremor-Dominant Parkinson's disease", "sort_order": 1},
    {"disease_type_id": 11, "disease_id": 4, "type_name": "Akinetic-Rigid",  "description": "Akinetic-Rigid Parkinson's disease", "sort_order": 2},
    {"disease_type_id": 12, "disease_id": 4, "type_name": "PIGD",            "description": "Postural Instability-Gait Difficulty", "sort_order": 3},
    # NMOSD subtypes (disease_id=5)
    {"disease_type_id": 13, "disease_id": 5, "type_name": "AQP4-positive",    "description": "AQP4-IgG seropositive NMOSD", "sort_order": 1},
    {"disease_type_id": 14, "disease_id": 5, "type_name": "AQP4-negative",    "description": "AQP4-IgG seronegative NMOSD", "sort_order": 2},
    {"disease_type_id": 15, "disease_id": 5, "type_name": "Double-negative",  "description": "AQP4-IgG and MOG-IgG double-negative NMOSD", "sort_order": 3},
    # Alzheimer subtypes (disease_id=7)
    {"disease_type_id": 16, "disease_id": 7, "type_name": "Early-onset", "description": "Early-onset Alzheimer's disease (before age 65)", "sort_order": 1},
    {"disease_type_id": 17, "disease_id": 7, "type_name": "Late-onset",  "description": "Late-onset Alzheimer's disease (age 65+)", "sort_order": 2},
]

DISEASES_SEED = [
    {"disease_id": 1, "disease_name": "Control",            "disease_code": None,     "description": "Healthy control subject (no neurological disease)"},
    {"disease_id": 2, "disease_name": "Multiple Sclerosis", "disease_code": "G35",    "description": "Chronic autoimmune demyelinating disease of the central nervous system"},
    {"disease_id": 3, "disease_name": "ALS",                "disease_code": "G12.21", "description": "Amyotrophic lateral sclerosis"},
    {"disease_id": 4, "disease_name": "Parkinson",          "disease_code": "G20",    "description": "Parkinson's disease"},
    {"disease_id": 5, "disease_name": "NMOSD",              "disease_code": "G36.0",  "description": "Neuromyelitis optica spectrum disorder"},
    {"disease_id": 6, "disease_name": "MOGAD",              "disease_code": "G36.1",  "description": "Myelin oligodendrocyte glycoprotein antibody-associated disease"},
    {"disease_id": 7, "disease_name": "Alzheimer",          "disease_code": "G30",    "description": "Alzheimer's disease"},
    {"disease_id": 8, "disease_name": "Dementia",           "disease_code": "F03",    "description": "Unspecified dementia"},
]


def _needs_migration(conn: Connection) -> bool:
    inspector = inspect(conn)
    tables = set(inspector.get_table_names())

    if "diseases" not in tables:
        return True

    disease_count = conn.execute(text("SELECT COUNT(*) FROM diseases")).scalar() or 0
    if disease_count == 0:
        return True

    if "disease_types" in tables:
        dt_count = conn.execute(text("SELECT COUNT(*) FROM disease_types")).scalar() or 0
        # Check if any seed rows are missing
        for row in DISEASE_TYPES_SEED:
            exists = conn.execute(
                text("SELECT 1 FROM disease_types WHERE disease_type_id = :id"),
                {"id": row["disease_type_id"]},
            ).scalar()
            if not exists:
                return True

    return False


def _seed_diseases(conn: Connection) -> int:
    inserted = 0
    for row in DISEASES_SEED:
        exists = conn.execute(
            text("SELECT 1 FROM diseases WHERE disease_id = :id"),
            {"id": row["disease_id"]},
        ).scalar()
        if exists:
            continue

        cols = ["disease_id", "disease_name"]
        vals = {"disease_id": row["disease_id"], "disease_name": row["disease_name"]}
        if row.get("disease_code") is not None:
            cols.append("disease_code")
            vals["disease_code"] = row["disease_code"]
        if row.get("description") is not None:
            cols.append("description")
            vals["description"] = row["description"]

        col_list = ", ".join(cols)
        val_list = ", ".join(f":{c}" for c in cols)
        conn.execute(text(f"INSERT INTO diseases ({col_list}) VALUES ({val_list})"), vals)
        inserted += 1

    if inserted:
        logger.info("Seeded %d disease rows", inserted)
    return inserted


def _seed_disease_types(conn: Connection) -> int:
    inserted = 0
    for row in DISEASE_TYPES_SEED:
        exists = conn.execute(
            text("SELECT 1 FROM disease_types WHERE disease_type_id = :id"),
            {"id": row["disease_type_id"]},
        ).scalar()
        if exists:
            continue

        cols = ["disease_type_id", "disease_id", "type_name"]
        vals = {
            "disease_type_id": row["disease_type_id"],
            "disease_id": row["disease_id"],
            "type_name": row["type_name"],
        }
        if row.get("description") is not None:
            cols.append("description")
            vals["description"] = row["description"]
        if row.get("sort_order") is not None:
            cols.append("sort_order")
            vals["sort_order"] = row["sort_order"]

        col_list = ", ".join(cols)
        val_list = ", ".join(f":{c}" for c in cols)
        conn.execute(text(f"INSERT INTO disease_types ({col_list}) VALUES ({val_list})"), vals)
        inserted += 1

    if inserted:
        logger.info("Seeded %d disease type rows", inserted)
    return inserted


def run_migration(engine: Engine, *, dry_run: bool = False) -> dict:
    t0 = time.time()
    changes: list[str] = []

    with engine.begin() as conn:
        seeded = _seed_diseases(conn)
        if seeded:
            changes.append(f"Seeded {seeded} disease rows")

        seeded_types = _seed_disease_types(conn)
        if seeded_types:
            changes.append(f"Seeded {seeded_types} disease type rows")

        # Reset PostgreSQL sequences to avoid PK collisions after explicit-ID inserts
        if seeded or seeded_types:
            dialect = conn.dialect.name if hasattr(conn, "dialect") else str(engine.dialect.name)
            if dialect == "postgresql":
                if seeded:
                    conn.execute(text(
                        "SELECT setval(pg_get_serial_sequence('diseases', 'disease_id'), "
                        "(SELECT COALESCE(MAX(disease_id), 0) + 1 FROM diseases), false)"
                    ))
                if seeded_types:
                    conn.execute(text(
                        "SELECT setval(pg_get_serial_sequence('disease_types', 'disease_type_id'), "
                        "(SELECT COALESCE(MAX(disease_type_id), 0) + 1 FROM disease_types), false)"
                    ))

        if dry_run:
            conn.rollback()

    elapsed = time.time() - t0
    return {
        "success": True,
        "already_migrated": not changes,
        "changes_made": changes,
        "elapsed_seconds": elapsed,
    }
