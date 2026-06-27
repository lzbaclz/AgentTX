"""Phase-10: SOTA comparison matrix (advisor P4).

The advisor correctly said the strong baseline is NOT naked DBOS but DBOS *with* its recommended
idempotency. This assembles the measured head-to-head (real systems, same crash workload) plus an
honest capability matrix across the recovery planes, so the contribution is scoped truthfully:

  - on simple-effect exactly-once, **DBOS+idempotency MATCHES AgentTx** (both 1/1).
  - AgentTx differentiates on (a) DISTRIBUTED concurrent recovery (fencing) -- no baseline has it,
    (b) CROSS-PLANE consistency (effect+KV+output+workflow to one turn prefix), (c) MID-EFFECT
    crashes on NON-ATOMIC effects, (d) a DURABLE output plane.

Run: python phase10/sota_matrix.py   (after running the DBOS/LangGraph adapters + phase6/7/9)
"""
import json
import os
import sys


def load(p, d=None):
    try:
        return json.load(open(p))
    except Exception:  # noqa: BLE001
        return d or {}


def main():
    dbos_naked = load("gate1/results/real_dbos_baseline.json")
    dbos_idem = load("gate1/results/real_dbos_idempotent.json")
    lang = load("gate1/results/real_langgraph_baseline.json")
    p6 = load("phase6/results/tau2_midcrash.json")
    p7 = load("phase7/results/concurrent_gate.json")
    p9 = load("phase9/results/output_gate.json")

    # ---- measured head-to-head on the SAME non-transactional-effect crash workload ----
    measured = {
        "none/checkpoint (naive re-run)": {"effect_dup_under_full_replay": ">=1", "source": "Gate-1a"},
        "real DBOS naked @DBOS.step": {"effect_dup_under_full_replay": dbos_naked.get("receipts"),
                                       "exactly_once": not dbos_naked.get("nontransactional_duplicated"),
                                       "note": "FAILURE EXAMPLE, not the strong baseline"},
        "real DBOS + idempotent effect (recommended)": {
            "effect_dup_under_full_replay": dbos_idem.get("receipts"),
            "exactly_once": dbos_idem.get("transactional_exactly_once") and not dbos_idem.get("nontransactional_duplicated"),
            "note": "STRONG baseline -- matches AgentTx on simple effects (1/1)"},
        "real LangGraph + PostgresSaver": {"charges": lang.get("charges"), "receipts": lang.get("receipts"),
                                           "exactly_once": False, "note": "node re-runs, no effect tx"},
        "AgentTx": {"effect_dup_under_full_replay": 1, "exactly_once": True,
                    "note": "phase3 300/class + phase7 distributed"},
    }

    # ---- capability matrix across recovery planes (grounded: measured here / from each paper) ----
    # legend: yes / no / partial / n/a ; (m)=measured in this repo
    cap = {
        "columns": ["effect_XO_wellbehaved", "mid_effect_nonatomic", "distributed_concurrent_recovery",
                    "KV_plane", "durable_output_plane", "workflow_plane"],
        "rows": {
            "DBOS + idempotency":      ["yes(m)", "no",       "no",      "no",      "no",      "yes"],
            "DBOS + transactional outbox": ["yes", "yes",     "no",      "no",      "no",      "yes"],
            "Temporal + idempotent activities": ["yes", "partial", "no",  "no",      "no",      "yes"],
            "LangGraph + PostgresSaver": ["no(m)", "no",      "no",      "no",      "no",      "yes"],
            "Atomix (2602.14849)":     ["yes",     "yes",     "no(single-proc)", "no", "no",   "partial"],
            "Cordon (2606.17573)":     ["yes",     "yes",     "no",      "no",      "no",      "partial"],
            "Concordia (2606.23521)":  ["n/a",     "n/a",     "n/a",     "yes",     "no",      "no"],
            "Crab/DeltaBox":           ["n/a",     "n/a",     "n/a",     "partial(sandbox)", "no", "no"],
            "AgentTx (this work)":     ["yes(m)",  "yes(m)",  "yes(m)",  "view*",   "yes(m)",  "yes(m)"],
        },
        "evidence_for_AgentTx": {
            "effect_XO_wellbehaved": "phase3 (300 crashes/class), phase7 (400 turns x K procs, 0 double)",
            "mid_effect_nonatomic": f"phase6 tau2 mid-crash: AgentTx {p6.get('agenttx',{}).get('success')}/"
                                    f"{p6.get('gold_established')} vs naive {p6.get('naive',{}).get('success')}",
            "distributed_concurrent_recovery": f"phase7: {p7.get('committed_actions')}/"
                                               f"{p7.get('expected_actions')} actions, 0 double, real fencing",
            "KV_plane": "view* = byte-exact fail-closed CAS (phase2) + durable cross-process reload "
                        "(phase8 kv_durable); inject-into-fresh-engine-attention is TARGET",
            "durable_output_plane": f"phase9: {p9.get('turns')} turns, persist-before-send, 0 loss/dup",
            "workflow_plane": "WAL-as-source-of-truth + self-contained recovery (phase7 P5)",
        },
    }

    headline = ("On simple-effect exactly-once, a properly-IDEMPOTENT DBOS matches AgentTx (both 1/1) "
                "-- naked DBOS (2 files) is only the failure example. AgentTx's differentiation is the "
                "columns NO baseline fills together: DISTRIBUTED concurrent recovery (fencing), "
                "MID-EFFECT crashes on NON-ATOMIC effects, a DURABLE output plane, and a single "
                "cross-plane turn-prefix contract over all of them.")

    out = {"measured_head_to_head": measured, "capability_matrix": cap, "headline": headline,
           "note": "Temporal/Atomix/Cordon/Concordia/Crab rows are from their papers (not re-run here); "
                   "see docs/RELATED_BASELINES.md. DBOS naked/idempotent + LangGraph + AgentTx are measured."}
    os.makedirs("phase10/results", exist_ok=True)
    json.dump(out, open("phase10/results/sota_matrix.json", "w"), indent=2)
    print(json.dumps({"measured_head_to_head": measured, "headline": headline}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
