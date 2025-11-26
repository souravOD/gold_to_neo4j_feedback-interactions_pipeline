from contextlib import contextmanager
from typing import Iterator, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool


class PostgresPool:
    """Minimal connection pool for Supabase Postgres (Gold layer)."""

    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 5):
        self._pool = SimpleConnectionPool(minconn, maxconn, dsn=dsn, cursor_factory=RealDictCursor)

    @contextmanager
    def connection(self) -> Iterator[psycopg2.extensions.connection]:
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def close(self) -> None:
        self._pool.closeall()


def fetch_one(conn, query: str, params: Optional[tuple] = None):
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchone()


def fetch_all(conn, query: str, params: Optional[tuple] = None):
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchall()


def execute(conn, query: str, params: Optional[tuple] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(query, params or ())
    conn.commit()
