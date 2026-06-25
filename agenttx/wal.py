"""Turn WAL: the durable, append-only source of truth for a turn (Postgres or SQLite)."""
from __future__ import annotations


class TurnWAL:
    def __init__(self, db):
        self.db = db
        pk = ("lsn BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY" if db.backend == "postgres"
              else "lsn INTEGER PRIMARY KEY AUTOINCREMENT")
        db.execute(f"CREATE TABLE IF NOT EXISTS wal({pk}, session TEXT, turn TEXT, "
                   f"type TEXT, key TEXT)")
        db.commit()

    def append(self, session, turn, type, key=""):
        self.db.execute("INSERT INTO wal(session,turn,type,key) VALUES(?,?,?,?)",
                        (session, turn, type, key))
        self.db.commit()

    def has(self, session, turn, type):
        return self.db.fetchone("SELECT 1 FROM wal WHERE session=? AND turn=? AND type=? LIMIT 1",
                                (session, turn, type)) is not None

    def records(self, session, turn):
        return self.db.execute("SELECT lsn,type,key FROM wal WHERE session=? AND turn=? "
                               "ORDER BY lsn", (session, turn)).fetchall()
