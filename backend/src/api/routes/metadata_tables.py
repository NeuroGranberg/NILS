"""Metadata table query API routes.

Performance optimized: Table list endpoint is cached for 30 seconds
to avoid expensive pg_catalog queries on every request.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from fastapi import APIRouter, HTTPException
from sqlalchemy import func, or_, select
from sqlalchemy import types as sqltypes

from metadata_db.session import SessionLocal as MetadataSessionLocal
from api.metadata_tables import get_table, list_tables, TableDefinition
from api.models.database import MetadataTableInfo, MetadataTableColumnInfo
from api.models.common import DataTablesRequest, DataTablesResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/metadata/tables", tags=["metadata-tables"])

# Cache for table list (avoids expensive pg_catalog queries)
_table_info_cache: dict[str, tuple[list[MetadataTableInfo], float]] = {}
_CACHE_TTL_SECONDS = 30.0


def _column_searchable(column) -> bool:
    """Check if a column type is searchable."""
    column_type = column.type
    if isinstance(column_type, sqltypes.String) or isinstance(column_type, sqltypes.Text):
        return True
    return False


def _estimate_row_count(session, table) -> int:
    """Estimate row count for a table, using PostgreSQL stats if available."""
    from sqlalchemy import text, bindparam
    from sqlalchemy.exc import SQLAlchemyError
    
    bind = session.get_bind()
    if bind is not None and bind.dialect.name == "postgresql":
        relname = table.name
        schema_name = table.schema
        stats_stmt = text(
            """
            SELECT
                COALESCE(c.reltuples, 0)::bigint AS reltuples,
                COALESCE(s.n_live_tup, 0)::bigint AS live_tup,
                COALESCE(s.n_mod_since_analyze, 0)::bigint AS mod_since,
                s.last_analyze
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_stat_all_tables s ON s.relid = c.oid
            WHERE c.relname = :relname
              AND (:schemaname IS NULL OR n.nspname = :schemaname)
            LIMIT 1
            """
        ).bindparams(
            bindparam("relname", type_=sqltypes.Text),
            bindparam("schemaname", type_=sqltypes.Text),
        )
        try:
            execution_bind = bind.execution_options(isolation_level="AUTOCOMMIT")
            with execution_bind.connect() as connection:
                stats = connection.execute(
                    stats_stmt,
                    {"relname": relname, "schemaname": schema_name},
                ).mappings().first()
            if stats:
                estimate = int(max(stats.get("reltuples") or 0, 0))
                live_tup = int(max(stats.get("live_tup") or 0, 0))
                mod_since = int(max(stats.get("mod_since") or 0, 0))
                stale = False
                if estimate < 50000:  # _ROW_COUNT_ESTIMATE_THRESHOLD
                    stale = True
                elif stats.get("last_analyze") is None:
                    stale = True
                elif estimate > 0 and mod_since > estimate * 0.2:  # _ROW_COUNT_MOD_THRESHOLD
                    stale = True
                elif live_tup and abs(estimate - live_tup) > live_tup * 0.25:
                    stale = True

                if not stale:
                    return estimate
        except Exception:
            pass

    count_stmt = select(func.count()).select_from(table)
    try:
        return int(session.execute(count_stmt).scalar_one() or 0)
    except SQLAlchemyError:
        return 0


def _build_table_info(definition: TableDefinition) -> MetadataTableInfo:
    """Build table info from table definition."""
    with MetadataSessionLocal() as session:
        total = _estimate_row_count(session, definition.columns[0].table)

    columns = [
        MetadataTableColumnInfo(
            name=column.name,
            label=column.name.replace("_", " "),
            type=column.type.__class__.__name__,
            searchable=_column_searchable(column),
            orderable=True,
        )
        for column in definition.columns
    ]

    return MetadataTableInfo(
        name=definition.name,
        label=definition.label,
        row_count=total,
        columns=columns,
    )


def _apply_datatables_filters(definition: TableDefinition, payload: DataTablesRequest):
    """Apply DataTables search filters."""
    columns_by_name = {column.name: column for column in definition.columns}
    searchable_columns = [column for column in definition.columns if _column_searchable(column)]

    filters = []
    search_value = (payload.search.value.strip() if payload.search and payload.search.value else "")
    if search_value:
        pattern = f"%{search_value.lower()}%"
        for column in searchable_columns:
            filters.append(func.lower(column).like(pattern))
    return columns_by_name, filters


def _ordering_for_request(definition: TableDefinition, payload: DataTablesRequest, columns_by_name: dict[str, Any]):
    """Build ordering clauses from DataTables request."""
    order_clauses = []
    for order in payload.order or []:
        if order.column < 0 or order.column >= len(payload.columns):
            continue
        column_name = payload.columns[order.column].data or payload.columns[order.column].name
        if not column_name:
            continue
        sa_column = columns_by_name.get(column_name)
        if sa_column is None:
            continue
        if order.dir.lower() == "desc":
            order_clauses.append(sa_column.desc())
        else:
            order_clauses.append(sa_column.asc())
    if not order_clauses and definition.default_order:
        order_clauses = [col.asc() for col in definition.default_order]
    return order_clauses


@router.get("", response_model=list[MetadataTableInfo])
def list_metadata_tables():
    """List all available metadata tables with column info (cached for 30s)."""
    cache_key = "metadata_tables"
    now = time.time()

    # Check cache
    if cache_key in _table_info_cache:
        cached_data, timestamp = _table_info_cache[cache_key]
        if now - timestamp < _CACHE_TTL_SECONDS:
            return cached_data

    # Build fresh data
    result = [_build_table_info(definition) for definition in list_tables()]
    _table_info_cache[cache_key] = (result, now)
    return result


@router.post("/{table_name}/query", response_model=DataTablesResponse)
def query_metadata_table(table_name: str, payload: DataTablesRequest):
    """Query a metadata table with DataTables-style pagination, filtering, and sorting."""
    try:
        definition = get_table(table_name)
    except KeyError:
        raise HTTPException(status_code=404, detail="Unknown metadata table")

    length = min(max(payload.length, 1), 500)
    start = max(payload.start, 0)

    columns_by_name, filters = _apply_datatables_filters(definition, payload)
    order_clauses = _ordering_for_request(definition, payload, columns_by_name)

    table = definition.columns[0].table
    base_select = select(*definition.columns)
    count_select = select(func.count()).select_from(table)

    if filters:
        filter_clause = or_(*filters)
        base_select = base_select.where(filter_clause)
        filtered_count_stmt = select(func.count()).select_from(table).where(filter_clause)
    else:
        filtered_count_stmt = None

    if order_clauses:
        base_select = base_select.order_by(*order_clauses)

    base_select = base_select.offset(start).limit(length)

    with MetadataSessionLocal() as session:
        total = session.scalar(count_select) or 0
        if filtered_count_stmt is not None:
            filtered_total = session.scalar(filtered_count_stmt) or 0
        else:
            filtered_total = total
        result = session.execute(base_select).mappings().all()

    data = [dict(row) for row in result]

    return DataTablesResponse(
        draw=payload.draw,
        recordsTotal=int(total),
        recordsFiltered=int(filtered_total),
        data=data,
    )
