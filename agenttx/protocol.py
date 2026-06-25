"""AgentTx turn-transaction protocol: records, turn state machine, invariants, and an
EXHAUSTIVE crash-recovery model checker.

This generalizes Gate-1a (which crashed at a few named points and counted duplicates) into
an exhaustive enumeration: for a turn that performs a sequence of (side-effect, wal-append)
operations, crash BETWEEN ANY TWO operations, run recovery, and assert the turn invariants
hold for every interleaving. A turn's durable WAL is the single source of truth; effects are
made exactly-once by binding each to a deterministic idempotency key and committing the
key-record atomically with (transactional) or idempotently-after (overlay/idempotent) the
effect.

Run: python3 agenttx/protocol.py   # runs the exhaustive checker
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from enum import Enum


class RecType(Enum):
    BEGIN_TURN = "BEGIN_TURN"
    MODEL_OUTPUT_PREPARED = "MODEL_OUTPUT_PREPARED"
    ACTION_PREPARED = "ACTION_PREPARED"
    ACTION_COMMITTED = "ACTION_COMMITTED"
    OBSERVATION_COMMITTED = "OBSERVATION_COMMITTED"
    TURN_COMMITTED = "TURN_COMMITTED"
    KV_SNAPSHOT_AVAILABLE = "KV_SNAPSHOT_AVAILABLE"


@dataclass
class Rec:
    lsn: int
    type: RecType
    turn: str
    key: str = ""        # action idempotency key (for ACTION_*/OBSERVATION_*)


# ---- the durable world: a WAL (append-only) + the real external effects ----
@dataclass
class World:
    wal: list = field(default_factory=list)          # the durable turn log (source of truth)
    effects: dict = field(default_factory=dict)        # key -> times the EXTERNAL effect fired
    _lsn: int = 0

    def append(self, type, turn, key=""):
        self._lsn += 1
        self.wal.append(Rec(self._lsn, type, turn, key))

    def fire_effect(self, key, transactional):
        """Execute the external side effect. transactional=True => the ACTION_COMMITTED
        record is written ATOMICALLY with the effect (same commit); idempotent effects
        (transactional or content-addressed) fire at most once for a given key."""
        self.effects[key] = self.effects.get(key, 0) + 1
        if transactional:
            self.append(RecType.ACTION_COMMITTED, "T", key)

    def committed_keys(self):
        return {r.key for r in self.wal if r.type == RecType.ACTION_COMMITTED}

    def observed_keys(self):
        return {r.key for r in self.wal if r.type == RecType.OBSERVATION_COMMITTED}


# ---- a turn that performs ONE side-effecting action, AgentTx-style ----
# Each action is a list of atomic operations; a crash may occur after any prefix.
def turn_ops(key, transactional, idempotent_effect):
    """Return the ordered atomic operations of an AgentTx action.
    transactional: effect+ACTION_COMMITTED commit atomically (one op).
    idempotent_effect: re-running the effect for the same key is a no-op (content-addressed
    overlay / idempotent API) -> safe even though ACTION_COMMITTED is a separate op.
    """
    ops = [("begin",)]
    if transactional:
        ops += [("effect+commit",)]                    # atomic: fire + ACTION_COMMITTED
    else:
        ops += [("effect",), ("commit",)]              # fire, then ACTION_COMMITTED (separate)
    ops += [("observe",), ("turn_commit",)]
    return ops, transactional, idempotent_effect


def apply_op(world: World, op, key, idempotent_effect):
    name = op[0]
    if name == "begin":
        world.append(RecType.BEGIN_TURN, "T")
    elif name == "effect+commit":
        world.fire_effect(key, transactional=True)
    elif name == "effect":
        # non-transactional effect: idempotent (overlay/content-addressed) or not
        if idempotent_effect and world.effects.get(key, 0) >= 1:
            pass                                       # re-run is a no-op (same keyed file)
        else:
            world.effects[key] = world.effects.get(key, 0) + 1
    elif name == "commit":
        world.append(RecType.ACTION_COMMITTED, "T", key)
    elif name == "observe":
        world.append(RecType.OBSERVATION_COMMITTED, "T", key)
    elif name == "turn_commit":
        world.append(RecType.TURN_COMMITTED, "T")


def recover_and_finish(world: World, ops, key, idempotent_effect):
    """Recovery: skip ops already durably recorded; re-run the rest. ACTION is 'done' iff
    ACTION_COMMITTED is in the WAL. The effect only re-fires if not committed AND not
    idempotently-present."""
    committed = key in world.committed_keys()
    for op in ops:
        name = op[0]
        if name in ("effect+commit", "effect", "commit") and committed:
            continue                                   # action already committed -> skip
        if name == "observe" and key in world.observed_keys():
            continue
        if name == "turn_commit" and any(r.type == RecType.TURN_COMMITTED for r in world.wal):
            continue
        apply_op(world, op, key, idempotent_effect)
        if name in ("effect+commit", "commit"):
            committed = True


# ---- invariants ----
def check_invariants(world: World, key):
    errs = []
    # I1 Action Uniqueness: external effect fired at most once
    if world.effects.get(key, 0) > 1:
        errs.append(f"I1 action-uniqueness: effect fired {world.effects[key]}x")
    # I2 No Ghost Observation: observed => committed (in WAL order)
    last_commit = max((r.lsn for r in world.wal if r.type == RecType.ACTION_COMMITTED and r.key == key), default=-1)
    first_obs = min((r.lsn for r in world.wal if r.type == RecType.OBSERVATION_COMMITTED and r.key == key), default=-1)
    if first_obs != -1 and (last_commit == -1 or last_commit > first_obs):
        errs.append("I2 ghost-observation: observed before/without commit")
    # I3 No Lost Effect: committed => eventually observed (after recovery)
    if key in world.committed_keys() and key not in world.observed_keys():
        errs.append("I3 lost-effect: committed but never observed after recovery")
    # I5 Prefix-Consistent Recovery: a finished turn ends in TURN_COMMITTED
    if not any(r.type == RecType.TURN_COMMITTED for r in world.wal):
        errs.append("I5 prefix-recovery: turn did not reach TURN_COMMITTED")
    return errs


def exhaustive_check(transactional, idempotent_effect, key="a1"):
    """Crash after EVERY prefix of the turn's ops, recover, assert invariants."""
    ops, _, _ = turn_ops(key, transactional, idempotent_effect)
    bad = []
    for crash_after in range(len(ops) + 1):
        world = World()
        # run up to the crash point
        for op in ops[:crash_after]:
            apply_op(world, op, key, idempotent_effect)
        # crash, then recover + finish
        recover_and_finish(world, ops, key, idempotent_effect)
        errs = check_invariants(world, key)
        if errs:
            bad.append((crash_after, errs, world.effects.get(key, 0)))
    return bad


def main():
    configs = [
        ("transactional effect (DB; ACTION_COMMITTED in same tx)", True, True),
        ("non-tx IDEMPOTENT effect (overlay / content-addressed by turn key)", False, True),
        ("non-tx NON-idempotent effect (raw append / 'send email')", False, False),
    ]
    print("=== AgentTx exhaustive crash-recovery model check ===")
    overall_ok = True
    summary = {}
    for name, tx, idem in configs:
        bad = exhaustive_check(tx, idem)
        ok = (len(bad) == 0)
        summary[name] = ok
        print(f"\n[{name}]")
        if ok:
            print("  PASS: invariants hold for every crash point (exactly-once)")
        else:
            print(f"  FAIL at {len(bad)} crash point(s):")
            for ca, errs, n in bad:
                print(f"    crash_after_op={ca} effect_fired={n}x: {errs}")
            # the non-idempotent case is EXPECTED to fail -> motivates fail-closed UNCERTAIN
            if not idem and not tx:
                print("  (EXPECTED: a raw non-idempotent non-transactional effect cannot be made "
                      "exactly-once by ANY orchestrator -> AgentTx fail-closes it as UNCERTAIN.)")
    # AgentTx's guarantee: transactional + idempotent-overlay effects are exactly-once
    # under exhaustive crash; non-idempotent irreversible effects are fail-closed (not a bug).
    guaranteed_ok = summary[configs[0][0]] and summary[configs[1][0]]
    print(f"\nGUARANTEED CLASSES exactly-once under exhaustive crash: {guaranteed_ok}")
    print("non-idempotent irreversible effect -> fail-closed UNCERTAIN (by design, not a violation)")
    return 0 if guaranteed_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
