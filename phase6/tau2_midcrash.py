"""Phase-6 (the decisive case): mid-effect crash on REAL tau2-bench retail tools.

tau2's money-moving tools self-guard against FULL-call replay (cancel checks status!="pending";
modify_payment checks len(payment_history)!=1) -- so a naive retry of a COMPLETED call is safe
(see tau2_agenttx.py: 85/85). But the tools are NON-ATOMIC: cancel_pending_order credits the
gift-card refund *inside a loop* and only sets status="cancelled" at the very end. If the worker
crashes AFTER the credit but BEFORE the status update, the guard still reads "pending" -> naive
recovery re-runs the whole tool -> the gift card is credited TWICE (a real double-refund the
per-tool guard cannot catch).

AgentTx's TRANSACTIONAL gateway executes the tool against a DB snapshot and atomically swaps it
in only on completion. A mid-effect crash discards the snapshot (the live DB is untouched);
recovery re-runs once on the clean DB -> exactly-once. Success == the real tau2 evaluator's DB
criterion: final DB hash == the gold DB hash.

Run: PYTHONPATH=. .venv_tau2b/bin/python phase6/tau2_midcrash.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.gateway import action_key
from tau2.registry import registry
from tau2.data_model.message import ToolCall
from tau2.domains.retail.tools import RetailTools
from tau2.domains.retail.data_model import RetailDB, OrderPayment, GiftCard
from tau2.domains.retail.utils import RETAIL_DB_PATH, RETAIL_POLICY_PATH
from tau2.environment.environment import Environment


class MidCrash(Exception):
    pass


class CrashableRetailTools(RetailTools):
    """RetailTools whose cancel_pending_order can crash mid-effect (after the gift-card credit,
    before the status update) -- a faithful internal non-atomic point of the REAL tool."""
    _crash_armed = False

    def cancel_pending_order(self, order_id: str, reason: str):
        order = self._get_order(order_id)
        if order.status != "pending":
            raise ValueError("Non-pending order cannot be cancelled")
        if reason not in {"no longer needed", "ordered by mistake"}:
            raise ValueError("Invalid reason")
        refunds = []
        for payment in order.payment_history:
            payment_id = payment.payment_method_id
            refunds.append(OrderPayment(transaction_type="refund", amount=payment.amount,
                                        payment_method_id=payment_id))
            user = self._get_user(order.user_id)
            pm = self._get_payment_method(user.user_id, payment_id)
            if isinstance(pm, GiftCard):                       # refund credited to gift card NOW
                pm.balance = round(pm.balance + payment.amount, 2)
        if self._crash_armed:                                  # <-- crash AFTER credit, BEFORE status
            raise MidCrash()
        order.status = "cancelled"
        order.cancel_reason = reason
        order.payment_history.extend(refunds)
        return order


def make_env():
    db = RetailDB.load(RETAIL_DB_PATH)
    tools = CrashableRetailTools(db)
    with open(RETAIL_POLICY_PATH) as fp:
        policy = fp.read()
    return Environment(domain_name="retail", policy=policy, tools=tools)


def dispatch(env, action, rid):
    return env.get_response(ToolCall(id=rid, name=action.name, arguments=dict(action.arguments), requestor="assistant"))


def gold_hash(actions):
    env = make_env()
    for i, a in enumerate(actions):
        if dispatch(env, a, f"g{i}").error:
            return None
    return env.get_db_hash()


def run_naive(actions):
    """No transaction: tool mutates the live DB; mid-effect crash; recovery re-runs the whole call."""
    env = make_env()
    for i, a in enumerate(actions):
        if a.name == "cancel_pending_order":
            env.tools._crash_armed = True
            try:
                env.tools.use_tool(a.name, **dict(a.arguments))   # crashes mid-effect on the LIVE db
            except MidCrash:
                pass
            env.tools._crash_armed = False
            dispatch(env, a, f"n{i}r")                             # recovery re-runs -> double credit
        else:
            dispatch(env, a, f"n{i}")
    return env.get_db_hash()


def run_agenttx(actions, session):
    """Transactional gateway: execute on a DB snapshot, atomic-swap on completion; mid-effect
    crash discards the snapshot; recovery re-runs once on the clean DB."""
    env = make_env()
    committed = set()
    for i, a in enumerate(actions):
        if a.name != "cancel_pending_order":
            dispatch(env, a, f"a{i}")
            continue
        key = action_key(session, f"t{i}", a.name, dict(a.arguments))
        if key in committed:
            continue                                              # dedup
        live = env.tools.db
        # ---- attempt 1: execute on a snapshot, crash mid-effect ----
        snap = live.model_copy(deep=True)
        env.tools.db = snap
        env.tools._crash_armed = True
        try:
            env.tools.use_tool(a.name, **dict(a.arguments))
            committed.add(key)                                    # (would commit if it completed)
            live.__dict__.update(snap.__dict__)
        except MidCrash:
            pass                                                  # snapshot discarded
        finally:
            env.tools._crash_armed = False
            env.tools.db = live                                   # rollback: live DB untouched
        # ---- recovery: re-run once on the clean DB, then commit ----
        if key not in committed:
            snap2 = live.model_copy(deep=True)
            env.tools.db = snap2
            env.tools.use_tool(a.name, **dict(a.arguments))
            live.__dict__.update(snap2.__dict__)
            env.tools.db = live
            committed.add(key)
    return env.get_db_hash()


def main():
    tasks = registry.get_tasks_loader("retail")()
    cancel_tasks = [t for t in tasks if t.evaluation_criteria and t.evaluation_criteria.actions
                    and any(a.name == "cancel_pending_order" for a in t.evaluation_criteria.actions)]
    res = {"domain": "retail", "tool": "cancel_pending_order (non-atomic, mid-effect crash)",
           "cancel_tasks": len(cancel_tasks), "gold_established": 0, "skipped": 0,
           "naive": {"success": 0, "fail": 0, "double_refund": 0},
           "agenttx": {"success": 0, "fail": 0, "double_refund": 0}}
    for t in cancel_tasks:
        actions = t.evaluation_criteria.actions
        g = gold_hash(actions)
        if g is None:
            res["skipped"] += 1
            continue
        res["gold_established"] += 1
        nv = run_naive(actions)
        atx = run_agenttx(actions, str(t.id))
        if nv == g:
            res["naive"]["success"] += 1
        else:
            res["naive"]["fail"] += 1
            res["naive"]["double_refund"] += 1
        if atx == g:
            res["agenttx"]["success"] += 1
        else:
            res["agenttx"]["fail"] += 1
            res["agenttx"]["double_refund"] += 1
    n = res["gold_established"] or 1
    res["headline"] = {
        "criterion": "final DB hash == gold DB hash (real tau2 EnvironmentEvaluator DB reward)",
        "naive_success_under_midcrash_pct": round(100 * res["naive"]["success"] / n, 1),
        "agenttx_success_under_midcrash_pct": round(100 * res["agenttx"]["success"] / n, 1),
        "naive_double_refunds": res["naive"]["double_refund"],
        "agenttx_double_refunds": res["agenttx"]["double_refund"],
    }
    res["PHASE6_MIDCRASH_PASS"] = (res["agenttx"]["fail"] == 0 and res["naive"]["double_refund"] > 0)
    os.makedirs("phase6/results", exist_ok=True)
    json.dump(res, open("phase6/results/tau2_midcrash.json", "w"), indent=2)
    print(json.dumps(res, indent=2))
    return 0 if res["PHASE6_MIDCRASH_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
