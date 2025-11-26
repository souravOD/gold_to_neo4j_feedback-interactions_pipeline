from datetime import datetime
from typing import Iterable, List, Optional

from psycopg2.extras import RealDictCursor

from src.domain.models.events import OutboxEvent


def fetch_pending_events(
    conn,
    batch_size: int,
    max_attempts: Optional[int] = None,
    table_names: Optional[List[str]] = None,
    aggregate_types: Optional[List[str]] = None,
) -> List[OutboxEvent]:
    """Fetch a batch of pending outbox events with SKIP LOCKED to support concurrency."""
    filters = ["processed_at IS NULL"]
    params: List = []

    if max_attempts is not None:
        filters.append("attempts < %s")
        params.append(max_attempts)

    if table_names:
        filters.append("table_name = ANY(%s)")
        params.append(table_names)

    if aggregate_types:
        filters.append("aggregate_type = ANY(%s)")
        params.append(aggregate_types)

    where_clause = " AND ".join(filters)
    sql = f"""
    SELECT id, aggregate_type, table_name, op, aggregate_id, payload, created_at, attempts
    FROM outbox_events
    WHERE {where_clause}
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT %s;
    """
    params.append(batch_size)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [OutboxEvent(**row) for row in rows]


def mark_processed(conn, event_id) -> None:
    sql = "UPDATE outbox_events SET processed_at = NOW(), error_message = NULL WHERE id = %s;"
    with conn.cursor() as cur:
        cur.execute(sql, (event_id,))
    conn.commit()


def mark_failed(conn, event_id, error_message: str) -> None:
    sql = """
    UPDATE outbox_events
    SET attempts = attempts + 1,
        error_message = %s,
        processed_at = NULL
    WHERE id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (error_message[:1000], event_id))
    conn.commit()
