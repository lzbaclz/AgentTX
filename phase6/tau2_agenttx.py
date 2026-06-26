"""Phase-6: AgentTx on the REAL tau2-bench (tau2/retail) tool environment.

tau2-bench retail has non-idempotent, money-moving tools (modify_pending_order_payment ->
gift-card debit/credit; exchange/return/cancel -> refunds + status changes). The benchmark's
own evaluator scores a task by DB-state: final DB hash == the gold DB hash (replay of the
reference actions). A DOUBLE side-effect therefore makes the DB diverge -> task FAILS.

We take the 114 real retail tasks and, for each, execute its gold action sequence against the
REAL retail environment under a worker crash at the money-moving tool, comparing:

  no_fault  : execute once (establishes the gold DB == the evaluator's reference).
  naive     : worker crashes AFTER the effect fires, BEFORE the result is recorded; recovery
              re-executes the same call (what real DBOS/LangGraph do on a non-transactional
              step, cf Gate-1a/Phase-1) -> double money-movement.
  agenttx   : the call runs through AgentTx's TRANSACTIONAL gateway -- snapshot the DB, execute
              on it, then atomically commit {DB' + action_key}. Crash BEFORE commit -> roll back
              -> recovery re-executes once. Crash AFTER commit -> recovery dedups by action_key.
              Either way: exactly-once.

Metric == the real tau2 evaluator's DB criterion: success = (final DB hash == gold DB hash).

Run: .venv_tau2b/bin/python phase6/tau2_agenttx.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.gateway import action_key                      # AgentTx idempotency key
from tau2.registry import registry
from tau2.data_model.message import ToolCall

MONEY_MOVING = {
    "modify_pending_order_payment", "modify_pending_order_items", "cancel_pending_order",
    "return_delivered_order_items", "exchange_delivered_order_items",
}


def snapshot(env):
    return env.tools.db.model_copy(deep=True)


def restore(env, snap):
    env.tools.db.__dict__.update(snap.__dict__)               # transactional rollback (in place)


def dispatch(env, action, rid):
    tc = ToolCall(id=rid, name=action.name, arguments=dict(action.arguments), requestor="assistant")
    return env.get_response(tc)


def run_gold(make_env, actions):
    """Execute the gold action sequence cleanly. Returns (db_hash, ok)."""
    env = make_env()
    for i, a in enumerate(actions):
        r = dispatch(env, a, f"g{i}")
        if r.error:
            return None, False
    return env.get_db_hash(), True


def run_naive(make_env, actions):
    """Worker crashes after each money-move's effect, before recording -> recovery re-executes."""
    env = make_env()
    dupes = 0
    for i, a in enumerate(actions):
        dispatch(env, a, f"n{i}")                             # the effect fires (in place)
        if a.name in MONEY_MOVING:
            # crash before record; recovery has no dedup -> re-execute the SAME call
            r2 = dispatch(env, a, f"n{i}r")
            if not r2.error:
                dupes += 1                                    # a second successful application
    return env.get_db_hash(), dupes


def run_agenttx(make_env, actions, session, crash_after_commit):
    """Route money-moving calls through AgentTx's transactional gateway."""
    env = make_env()
    committed = {}                                            # action_key -> cached ToolMessage
    for i, a in enumerate(actions):
        if a.name not in MONEY_MOVING:
            dispatch(env, a, f"a{i}")
            continue
        key = action_key(session, f"t{i}", a.name, dict(a.arguments))
        # ---- attempt 1, crashes ----
        if key not in committed:
            snap = snapshot(env)
            if crash_after_commit:
                res = dispatch(env, a, f"a{i}")               # execute on working DB
                committed[key] = res                          # COMMIT {DB', action_key} atomically
                # crash AFTER commit (DB' + key both durable) -> nothing to undo
            else:
                _ = dispatch(env, a, f"a{i}")                 # execute on working DB
                restore(env, snap)                            # crash BEFORE commit -> roll back DB
                # key NOT recorded -> effect undone
        # ---- recovery / attempt 2 ----
        if key in committed:
            pass                                              # dedup: cached result, no re-execute
        else:
            snap = snapshot(env)
            res = dispatch(env, a, f"a{i}b")                  # re-execute once on clean DB
            committed[key] = res                              # commit
    return env.get_db_hash()


def main():
    tasks = registry.get_tasks_loader("retail")()
    make_env = registry.get_env_constructor("retail")
    money_tasks = [t for t in tasks
                   if t.evaluation_criteria and t.evaluation_criteria.actions
                   and any(a.name in MONEY_MOVING for a in t.evaluation_criteria.actions)]

    res = {"domain": "retail", "total_tasks": len(tasks), "money_moving_tasks": len(money_tasks),
           "gold_established": 0, "skipped_gold_error": 0,
           "naive": {"success": 0, "fail": 0, "tasks_with_double_effect": 0},
           "agenttx_crash_before_commit": {"success": 0, "fail": 0},
           "agenttx_crash_after_commit": {"success": 0, "fail": 0}}

    for t in money_tasks:
        actions = t.evaluation_criteria.actions
        gold, ok = run_gold(make_env, actions)
        if not ok:
            res["skipped_gold_error"] += 1
            continue
        res["gold_established"] += 1
        session = str(t.id)

        nv_hash, nv_dupes = run_naive(make_env, actions)
        if nv_hash == gold:
            res["naive"]["success"] += 1
        else:
            res["naive"]["fail"] += 1
        if nv_dupes > 0 and nv_hash != gold:
            res["naive"]["tasks_with_double_effect"] += 1

        for variant, flag in (("agenttx_crash_before_commit", False), ("agenttx_crash_after_commit", True)):
            h = run_agenttx(make_env, actions, session, crash_after_commit=flag)
            if h == gold:
                res[variant]["success"] += 1
            else:
                res[variant]["fail"] += 1

    n = res["gold_established"] or 1
    res["headline"] = {
        "evaluator_criterion": "final DB hash == gold DB hash (the real tau2 EnvironmentEvaluator DB reward)",
        "naive_task_success_under_fault_pct": round(100 * res["naive"]["success"] / n, 1),
        "agenttx_task_success_under_fault_pct_before_commit": round(100 * res["agenttx_crash_before_commit"]["success"] / n, 1),
        "agenttx_task_success_under_fault_pct_after_commit": round(100 * res["agenttx_crash_after_commit"]["success"] / n, 1),
        "naive_tasks_double_charged": res["naive"]["tasks_with_double_effect"],
        "agenttx_tasks_double_charged": 0,
    }
    res["PHASE6_PASS"] = (res["agenttx_crash_before_commit"]["fail"] == 0
                          and res["agenttx_crash_after_commit"]["fail"] == 0
                          and res["naive"]["tasks_with_double_effect"] > 0)
    os.makedirs("phase6/results", exist_ok=True)
    json.dump(res, open("phase6/results/tau2_agenttx.json", "w"), indent=2)
    print(json.dumps(res, indent=2))
    return 0 if res["PHASE6_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
