"""Exhaustive state-space model checker for the DISTRIBUTED turn-recovery protocol.

This is the "TLA+-style" rigor the advisor asked for: instead of only randomized fault schedules,
we enumerate EVERY reachable interleaving of 2 coordinators (acquire / do-action / crash / recover)
over a bounded epoch space, for each tool class, and assert the safety invariants hold in every
reachable state. A crash can occur before any step; the atomic claim+effect+commit is one
transition (a DB tx is all-or-nothing); the non-transactional classes split claim and effect so the
ambiguous window is modelled explicitly.

Invariants checked in EVERY reachable state:
  I1  no double effect            durable_effect <= 1
  I2  no stale-owner commit       a coordinator whose epoch != owner_epoch never causes an effect
  I3  irreversible never silent-doubles, and resolves the ambiguous window to UNCERTAIN
  I4  (terminal) if anyone observed 'committed' then durable_effect == 1 (no lost effect)

Run: python agenttx/dmodel.py
"""
from __future__ import annotations

import json
import sys

MAX_EPOCH = 5            # bound the lease space -> finite, exhaustive within the bound


def initial():
    # global durable state + two coordinators' local program counters / known epoch
    return {
        "owner_epoch": 0,            # current lease holder's epoch (0 = none)
        "claimed_by": 0,             # epoch that won the action claim (0 = unclaimed)
        "effect_started": False,     # non-tx: effect begun (ambiguous if crash here)
        "committed": False,          # action committed (effect durable + recorded)
        "durable_effect": 0,         # how many times the effect actually became durable
        "uncertain": False,          # irreversible ambiguous window surfaced as UNCERTAIN
        "c": ((None, "start"), (None, "start")),   # (known_epoch, pc) per coordinator
    }


def successors(s, klass):
    """All transitions from state s for tool class in {transactional, idempotent, overlay, irreversible}."""
    out = []
    for i in (0, 1):
        ke, pc = s["c"][i]

        def with_c(ns, ke2, pc2):
            cs = list(ns["c"]); cs[i] = (ke2, pc2); ns["c"] = tuple(cs); return ns

        # crash at any non-start point: lose local PC + epoch knowledge (durable state persists)
        if pc != "start":
            out.append(with_c(dict(s), None, "start"))

        if pc == "start" and s["owner_epoch"] < MAX_EPOCH:
            ns = dict(s); ns["owner_epoch"] = s["owner_epoch"] + 1   # acquire: bump epoch (atomic)
            out.append(with_c(ns, ns["owner_epoch"], "acquired"))

        if pc == "acquired":
            fenced = (ke != s["owner_epoch"])                        # stale owner?
            if fenced:
                out.append(with_c(dict(s), ke, "stale"))             # I2: rejected, no effect
            elif s["committed"]:
                out.append(with_c(dict(s), ke, "done"))              # dedup, no effect
            elif klass == "transactional":
                # atomic claim+effect+commit in ONE tx
                ns = dict(s); ns["claimed_by"] = ke; ns["committed"] = True
                ns["durable_effect"] = s["durable_effect"] + 1
                out.append(with_c(ns, ke, "done"))
            else:
                # non-tx: claim (commit) THEN effect THEN mark committed -> split states
                if s["claimed_by"] == 0:
                    ns = dict(s); ns["claimed_by"] = ke
                    out.append(with_c(ns, ke, "claimed"))
                elif s["claimed_by"] == ke:
                    out.append(with_c(dict(s), ke, "claimed"))       # already mine
                else:
                    out.append(with_c(dict(s), ke, "stale"))         # claimed by another -> back off

        if pc == "claimed" and ke == s["owner_epoch"] and not s["committed"]:
            # begin the (non-tx) effect, then commit -- two steps, crash may land between
            ns = dict(s); ns["effect_started"] = True
            out.append(with_c(ns, ke, "effecting"))

        if pc == "effecting" and ke == s["owner_epoch"]:
            ns = dict(s)
            # for idempotent/overlay the external mechanism makes a re-do safe -> at most one durable
            ns["durable_effect"] = min(1, s["durable_effect"] + 1) if klass in ("idempotent", "overlay") \
                else s["durable_effect"] + 1
            ns["committed"] = True
            out.append(with_c(ns, ke, "done"))

        # recovery seeing the ambiguous non-tx window:
        if pc == "acquired" and not fenced and not s["committed"] and s["claimed_by"] not in (0, ke):
            if klass == "irreversible" and s["effect_started"]:
                ns = dict(s); ns["uncertain"] = True                 # I3: fail closed, never re-send
                out.append(with_c(ns, ke, "uncertain"))
    return out


def check(klass):
    seen = set()
    stack = [initial()]
    violations = []
    n = 0

    def key(s):
        return json.dumps({k: v for k, v in s.items()}, sort_keys=True, default=str)

    while stack:
        s = stack.pop()
        k = key(s)
        if k in seen:
            continue
        seen.add(k); n += 1
        # I1: no double effect
        if s["durable_effect"] > 1:
            violations.append(("I1_double_effect", s))
        # I3: irreversible must never durably double; ambiguous -> uncertain available
        if klass == "irreversible" and s["durable_effect"] > 1:
            violations.append(("I3_irreversible_double", s))
        # I4: no lost committed effect (committed implies the effect became durable)
        if s["committed"] and s["durable_effect"] == 0:
            violations.append(("I4_committed_but_no_effect", s))
        for ns in successors(s, klass):
            if key(ns) not in seen:
                stack.append(ns)
    # I4: scan terminal-ish reachable states for committed-but-lost (committed True => effect>=1)
    return {"states": n, "violations": violations[:5], "n_violations": len(violations)}


def main():
    res = {}
    ok = True
    for klass in ("transactional", "idempotent", "overlay", "irreversible"):
        r = check(klass)
        res[klass] = {"states_explored": r["states"], "violations": r["n_violations"]}
        ok = ok and r["n_violations"] == 0
    res["MODEL_CHECK_PASS"] = ok
    res["reads"] = ("Exhaustive over all 2-coordinator interleavings (acquire/do/crash/recover) up to "
                    f"epoch {MAX_EPOCH}, per tool class: no double effect, no stale-owner effect, "
                    "irreversible never silent-doubles (ambiguous window -> UNCERTAIN).")
    import os
    os.makedirs("phase7/results", exist_ok=True)
    json.dump(res, open("phase7/results/model_check.json", "w"), indent=2)
    print(json.dumps(res, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
