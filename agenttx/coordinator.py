"""Recovery coordinator: drives a turn through the WAL + Tool Gateway; recovery re-runs the
turn and the gateway's per-class mechanism makes supported tools exactly-once."""
from __future__ import annotations

from agenttx.gateway import Gateway
from agenttx.wal import TurnWAL


class Coordinator:
    def __init__(self, db, store):
        self.db = db
        self.store = store
        self.wal = TurnWAL(db)
        self.gw = Gateway(db, store)

    def run_turn(self, session, turn, plan, clock):
        """plan = [(Tool, args_dict), ...]. Returns the per-action Results."""
        if not self.wal.has(session, turn, "BEGIN_TURN"):
            self.wal.append(session, turn, "BEGIN_TURN")
        clock.tick()
        results = []
        for tool, args in plan:
            self.wal.append(session, turn, "ACTION_PREPARED", tool.name); clock.tick()
            res = self.gw.call(session, turn, tool, args, clock)
            results.append(res)
            self.wal.append(session, turn, "OBSERVATION_COMMITTED", res.key); clock.tick()
        if not self.wal.has(session, turn, "TURN_COMMITTED"):
            self.wal.append(session, turn, "TURN_COMMITTED")
        clock.tick()
        return results

    def recover(self, session, turn, plan, clock):
        return self.run_turn(session, turn, plan, clock)
