"""Recovery Coordinator: on recovery, (1) re-run the turn's tool plan -- the Tool Gateway's
action-key dedup makes supported tools exactly-once -- and (2) decide the KV path via the
KV-View: RESTORE the byte-exact KV snapshot if provenance + checksums verify, else FAIL_CLOSED
and RECOMPUTE from the committed transcript. Correctness (tools + output) never depends on the
KV snapshot; the snapshot only accelerates recovery."""
from __future__ import annotations

from agenttx.kvview import KVView, Provenance


class RecoveryCoordinator:
    def __init__(self, coordinator, kvview: KVView):
        self.co = coordinator
        self.kv = kvview

    @staticmethod
    def _lsn(session, turn):
        return f"{session}/{turn}"

    def commit_turn(self, session, turn, plan, prov: Provenance, kv_blocks, clock):
        """Run the turn, then snapshot its KV as a materialized view of the (now durable) turn."""
        results = self.co.run_turn(session, turn, plan, clock)
        manifest = self.kv.snapshot(self._lsn(session, turn), prov, kv_blocks)
        return results, manifest

    def recover_turn(self, session, turn, plan, current_prov: Provenance, clock):
        # KV decision FIRST (fast path vs recompute), then re-run tools (gateway dedup).
        rr = self.kv.restore(self._lsn(session, turn), current_prov)
        kv_action = "restore" if rr.restored else f"recompute({rr.reason})"
        self.co.recover(session, turn, plan, clock)        # tools: exactly-once via gateway
        return {"kv_action": kv_action, "kv_restored": rr.restored,
                "kv_reason": rr.reason, "n_kv_blocks": len(rr.blocks)}
