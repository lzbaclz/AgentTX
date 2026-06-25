"""Thin DB adapter so the WAL + Tool Gateway run unchanged on SQLite (tests/gates) and
PostgreSQL (the production durable store + the substrate DBOS uses)."""
from __future__ import annotations

import contextlib
import os


class DB:
    def __init__(self, backend, conn, ph):
        self.backend = backend          # "sqlite" | "postgres"
        self.conn = conn
        self._ph = ph                    # param placeholder: "?" (sqlite) | "%s" (postgres)

    def q(self, sql):
        return sql.replace("?", self._ph) if self._ph != "?" else sql

    def execute(self, sql, params=()):
        cur = self.conn.cursor()
        cur.execute(self.q(sql), params)
        return cur

    def fetchone(self, sql, params=()):
        return self.execute(sql, params).fetchone()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        with contextlib.suppress(Exception):
            self.conn.rollback()

    def close(self):
        with contextlib.suppress(Exception):
            self.conn.close()

    @contextlib.contextmanager
    def transaction(self):
        """One atomic transaction (autocommit off). Used to commit an effect AND its
        action-key record together (the transactional exactly-once mechanism)."""
        try:
            yield self
            self.conn.commit()
        except Exception:
            self.rollback()
            raise


def open_sqlite(path):
    import sqlite3
    c = sqlite3.connect(path, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=FULL")
    return DB("sqlite", c, "?")


def open_postgres(dsn=None):
    import psycopg
    dsn = dsn or os.environ.get("AGENTTX_PG_DSN", "")
    c = psycopg.connect(dsn, autocommit=False)
    return DB("postgres", c, "%s")


def open_db(url):
    """url: 'sqlite:///path' or 'postgres://...' / 'postgresql://...'"""
    if url.startswith("sqlite:///"):
        return open_sqlite(url[len("sqlite:///"):])
    if url.startswith(("postgres://", "postgresql://")):
        return open_postgres(url)
    raise ValueError(f"unknown db url: {url}")
