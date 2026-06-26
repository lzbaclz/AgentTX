"""Streaming exactly-once + multi-worker resume.

The turn's committed output tokens live in a DURABLE log (the source of truth). A stateless
worker streams them to the client tagged (session, turn, seq). The client keeps the highest
CONTIGUOUS seq it has received (its ACK watermark) and dedups: a re-sent token (seq <= ack)
is dropped, an out-of-order token (seq > ack+1) is ignored (it will be re-sent in order). On
a worker crash, ANOTHER worker resumes from the server-known ACK (which may lag the client's
real ACK -> harmless re-sends that the client dedups). Guarantee: the client materializes
each committed token EXACTLY ONCE, in order, across any number of mid-stream crashes and
worker reroutes.
"""
from __future__ import annotations


class WorkerCrash(Exception):
    pass


class CrashClock:
    def __init__(self, kill_at=0):
        self.n = 0
        self.kill_at = kill_at

    def tick(self):
        self.n += 1
        if self.kill_at and self.n == self.kill_at:
            raise WorkerCrash()


class StreamLog:
    """Durable, append-only committed-token log for a turn. seq is 1-indexed."""
    def __init__(self, tokens):
        self.tokens = list(tokens)

    def __len__(self):
        return len(self.tokens)

    def range_from(self, seq):
        for i in range(seq, len(self.tokens) + 1):
            yield i, self.tokens[i - 1]


class StreamClient:
    def __init__(self):
        self.ack = 0            # highest CONTIGUOUS seq received
        self.received = []      # the materialized tokens, in order
        self.dup_dropped = 0

    def deliver(self, seq, token):
        if seq == self.ack + 1:           # the next in-order token -> accept
            self.received.append(token)
            self.ack = seq
        elif seq <= self.ack:             # already have it (a re-send) -> dedup/drop
            self.dup_dropped += 1
        # seq > ack+1: an out-of-order gap -> ignore; it is re-sent in order from ack+1


def serve(log: StreamLog, client: StreamClient, resume_from: int, clock: CrashClock):
    """A worker streams log[resume_from..] to the client; may crash mid-stream. The two ticks
    around deliver model in-flight uncertainty (crash before deliver = token lost in flight;
    crash after deliver = client has it but the ACK may not have reached the server)."""
    for seq, token in log.range_from(resume_from):
        clock.tick()                      # crash BEFORE deliver
        client.deliver(seq, token)
        clock.tick()                      # crash AFTER deliver
