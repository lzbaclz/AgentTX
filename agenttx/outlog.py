"""Durable output log (replaces the in-memory StreamLog prototype with persist-before-send).

The advisor flagged that streaming exactly-once was only a PROTOTYPE: StreamLog was a Python list
and the client ACK an in-memory field. This makes the output plane durable:

  * every output token is PERSISTED to a durable log (committed/fsync'd) BEFORE it is sent;
  * the client records what it received in its OWN durable table and dedups by seq;
  * the client ACK watermark = the highest contiguous seq the client has durably received;
  * after a stream-worker / coordinator crash, ANY worker resumes from the durable log at
    client_ack+1; after a CLIENT restart, the client re-reads its durable received set.

Invariant: every token the client ever saw was persisted first -> the client materializes each
committed output token exactly once, in order, with no loss, across worker death + client restart.
Tables are keyed so all of this is idempotent under concurrent resumers (Postgres or SQLite).
"""
from __future__ import annotations


class OutputLog:
    def __init__(self, db):
        self.db = db
        db.execute("CREATE TABLE IF NOT EXISTS output_chunks("
                   "session TEXT, turn TEXT, seq INTEGER, token TEXT, "
                   "PRIMARY KEY(session,turn,seq))")
        # the CLIENT's own durable record of what it has materialized (its source of truth)
        db.execute("CREATE TABLE IF NOT EXISTS client_received("
                   "session TEXT, turn TEXT, seq INTEGER, token TEXT, "
                   "PRIMARY KEY(session,turn,seq))")
        db.commit()

    # ---- producer side ----
    def persist(self, session, turn, seq, token):
        """Durably append a committed output token BEFORE it may be sent (idempotent)."""
        self.db.execute("INSERT INTO output_chunks(session,turn,seq,token) VALUES(?,?,?,?) "
                        "ON CONFLICT(session,turn,seq) DO NOTHING", (session, turn, seq, token))
        self.db.commit()

    def chunks_from(self, session, turn, seq):
        return self.db.execute("SELECT seq, token FROM output_chunks WHERE session=? AND turn=? "
                               "AND seq>=? ORDER BY seq", (session, turn, seq)).fetchall()

    def total(self, session, turn):
        r = self.db.fetchone("SELECT COUNT(*) FROM output_chunks WHERE session=? AND turn=?", (session, turn))
        return r[0] if r else 0

    # ---- client side ----
    def client_ack(self, session, turn):
        """Highest CONTIGUOUS seq the client has durably received (0 if none)."""
        rows = self.db.execute("SELECT seq FROM client_received WHERE session=? AND turn=? ORDER BY seq",
                               (session, turn)).fetchall()
        ack = 0
        for (s,) in rows:
            if s == ack + 1:
                ack = s
            elif s > ack + 1:
                break
        return ack

    def deliver(self, session, turn, seq, token):
        """Client durably records a received token, deduping by seq (idempotent)."""
        self.db.execute("INSERT INTO client_received(session,turn,seq,token) VALUES(?,?,?,?) "
                        "ON CONFLICT(session,turn,seq) DO NOTHING", (session, turn, seq, token))
        self.db.commit()

    def client_view(self, session, turn):
        return [t for (s, t) in self.db.execute(
            "SELECT seq, token FROM client_received WHERE session=? AND turn=? ORDER BY seq",
            (session, turn)).fetchall()]
