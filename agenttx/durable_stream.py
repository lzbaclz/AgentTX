"""Durable output streaming (persist-before-send) — upgrades the in-memory prototype
(`agenttx/stream.py`) to a DURABLE output log, so the client materializes the committed token
prefix EXACTLY ONCE across worker crashes, reroutes, coordinator+streamer co-death, lost ACKs, and
client restart — WITHOUT regenerating (a re-routed worker RE-SENDS logged tokens, it never re-decodes,
because free generation diverges across the KV-reuse boundary).

Durable state (the source of truth is the log, not any worker):
  output_log(session, turn, seq, token)         -- the committed output tokens, PERSISTED (fsync'd)
                                                   BEFORE any worker streams them (persist-before-send)
  stream_ack(session, turn, client_id, ack_seq) -- the server-known ACK watermark (may lag the client)

The CLIENT also persists what it has durably materialized (its own receipt log), so a worker death
does not lose delivered tokens, and a client *restart* is modeled by clearing it (then a fresh worker
replays from the durable output_log). The resume point is the CLIENT's true contiguous ACK: a worker
streams output_log[client_ack+1 ..]; re-sends (seq <= client_ack) are deduped by the client.
"""
from __future__ import annotations


class DurableStream:
    """Server side: the durable output log + the persisted server ACK watermark."""

    def __init__(self, db):
        self.db = db
        db.execute("CREATE TABLE IF NOT EXISTS output_log("
                   "session TEXT, turn TEXT, seq INTEGER, token TEXT, "
                   "PRIMARY KEY(session, turn, seq))")
        db.execute("CREATE TABLE IF NOT EXISTS stream_ack("
                   "session TEXT, turn TEXT, client_id TEXT, ack_seq INTEGER, "
                   "PRIMARY KEY(session, turn, client_id))")
        db.commit()

    def commit_output(self, session, turn, tokens):
        """PERSIST-BEFORE-SEND: durably commit the turn's output tokens (idempotent by seq) before
        any worker is allowed to stream. After this returns, any worker can (re-)send from the log."""
        for seq, tok in enumerate(tokens, 1):
            self.db.execute("INSERT OR IGNORE INTO output_log(session,turn,seq,token) VALUES(?,?,?,?)",
                            (session, turn, seq, tok))
        self.db.commit()

    def output_len(self, session, turn):
        return self.db.fetchone("SELECT COUNT(*) FROM output_log WHERE session=? AND turn=?",
                                (session, turn))[0]

    def tokens_from(self, session, turn, seq):
        return self.db.execute("SELECT seq, token FROM output_log WHERE session=? AND turn=? AND seq>=? "
                               "ORDER BY seq", (session, turn, seq)).fetchall()

    def server_ack(self, session, turn, client_id):
        r = self.db.fetchone("SELECT ack_seq FROM stream_ack WHERE session=? AND turn=? AND client_id=?",
                             (session, turn, client_id))
        return r[0] if r else 0

    def record_ack(self, session, turn, client_id, ack_seq):
        """Persist the server's knowledge of the client ACK (monotonic; never regress)."""
        self.db.execute(
            "INSERT INTO stream_ack(session,turn,client_id,ack_seq) VALUES(?,?,?,?) "
            "ON CONFLICT(session,turn,client_id) DO UPDATE SET "
            "ack_seq=MAX(stream_ack.ack_seq, excluded.ack_seq)",
            (session, turn, client_id, ack_seq))
        self.db.commit()


class DurableClient:
    """Client side: a durable receipt log + the contiguous ACK watermark. Dedups by seq."""

    def __init__(self, db, session, turn, client_id):
        self.db = db
        self.session, self.turn, self.client_id = session, turn, client_id
        db.execute("CREATE TABLE IF NOT EXISTS client_recv("
                   "session TEXT, turn TEXT, client_id TEXT, seq INTEGER, token TEXT, "
                   "PRIMARY KEY(session, turn, client_id, seq))")
        db.commit()

    def _rows(self):
        return self.db.execute(
            "SELECT seq, token FROM client_recv WHERE session=? AND turn=? AND client_id=? ORDER BY seq",
            (self.session, self.turn, self.client_id)).fetchall()

    def ack(self):
        """Highest CONTIGUOUS seq durably received (a gap stops the watermark)."""
        ack = 0
        for seq, _ in self._rows():
            if seq == ack + 1:
                ack = seq
            elif seq > ack + 1:
                break
        return ack

    def deliver(self, seq, token):
        """Materialize a delivered token durably; dedup re-sends by the seq primary key."""
        self.db.execute(
            "INSERT OR IGNORE INTO client_recv(session,turn,client_id,seq,token) VALUES(?,?,?,?,?)",
            (self.session, self.turn, self.client_id, seq, token))
        self.db.commit()

    def materialized(self):
        """The in-order tokens of the contiguous prefix the user has actually seen."""
        out, ack = [], 0
        for seq, tok in self._rows():
            if seq == ack + 1:
                out.append(tok); ack = seq
            elif seq > ack + 1:
                break
        return out

    def restart(self):
        """Model a client restart that loses its materialized state (ACK falls back to 0)."""
        self.db.execute("DELETE FROM client_recv WHERE session=? AND turn=? AND client_id=?",
                        (self.session, self.turn, self.client_id))
        self.db.commit()
