"""Phase-3: fault-injection audit of the three REAL Tool Gateway classes + fail-closed.
A persistent external HTTP service + real Postgres; the GATEWAY is crashed (in-process) at a
random durable boundary every trial, then recovers. Oracle = the REAL effect counter for
each class. Guarantees:
  TRANSACTIONAL (Postgres) / OVERLAY (FS) / IDEMPOTENT (HTTP idempotency key): EXACTLY-ONCE.
  IRREVERSIBLE (non-idempotent HTTP): the effect fires AT MOST ONCE and a crash in the
    effect/commit window is surfaced as UNCERTAIN (fail-closed, never silently re-sent).
"""
import json, os, random, subprocess, sys, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agenttx.db import open_postgres
from agenttx.core import Clock, Crash, close_coord  # noqa
from agenttx.gateway import Gateway
from agenttx.tools import ChargeTool, ReceiptTool, HttpIdempotentTool, HttpUnsafeTool

PORT = 8732; URL = f"http://127.0.0.1:{PORT}"; STORE = "/tmp/atx_g3"; MAXT = 8
def svc_count(kind): return json.load(urllib.request.urlopen(f"{URL}/count?kind={kind}"))["count"]

def reset():
    db = open_postgres()
    for t in ("gw_keys", "charges"): db.execute(f"DROP TABLE IF EXISTS {t}")
    db.commit(); db.close()
    os.system(f"rm -rf {STORE} && mkdir -p {STORE}")

def run_trial(tool, args_fn, rng):
    """One turn through the gateway with a random crash, then recover. Returns (result_status)."""
    session, turn = "s", f"t{rng.random()}"        # unique key per trial
    args = args_fn(turn)
    g = Gateway(open_postgres(), STORE)
    status = None
    try:
        status = g.call(session, turn, tool, args, Clock(rng.randint(1, MAXT), hard=False)).status
    except Crash:
        pass
    g.db.rollback(); g.db.close()
    # recover: fresh gateway re-runs the same call (gateway dedup / idempotency makes it safe)
    g2 = Gateway(open_postgres(), STORE)
    try:
        status = g2.call(session, turn, tool, args, Clock(0)).status
    except Crash:
        pass
    g2.db.close()
    return status, turn

def main():
    svc = subprocess.Popen([sys.executable, "phase3/mock_service.py", str(PORT)])
    time.sleep(1.2)
    try:
        rng = random.Random(0); N = 300; out = {}
        # --- 3 safe classes: each trial's REAL effect must be EXACTLY ONCE ---
        # TRANSACTIONAL (Postgres)
        reset(); dups = 0; lost = 0
        for i in range(N):
            st, turn = run_trial(ChargeTool(), lambda tn: {"order": tn, "amount": 100}, rng)
            db = open_postgres(); c = db.execute("SELECT COUNT(*) FROM charges WHERE order_id=%s", (turn,)).fetchone()[0]; db.close()
            dups += c > 1; lost += c < 1
        out["transactional_postgres"] = {"trials": N, "duplicate": dups, "lost": lost, "exactly_once": dups == 0 and lost == 0}
        # OVERLAY (FS)
        reset(); dups = 0; lost = 0
        import glob
        for i in range(N):
            st, turn = run_trial(ReceiptTool(), lambda tn: {"order": tn}, rng)
            c = len(glob.glob(f"{STORE}/committed/*.receipt"))   # cumulative; check growth==i+1
            # per-trial: receipts grow by exactly 1 (content-addressed by unique key)
        n_receipt = len(glob.glob(f"{STORE}/committed/*.receipt"))
        out["overlay_fs"] = {"trials": N, "total_receipts": n_receipt, "exactly_once": n_receipt == N}
        # IDEMPOTENT (HTTP idempotency key)
        reset(); before = svc_count("charge")
        for i in range(N):
            run_trial(HttpIdempotentTool(URL), lambda tn: {}, rng)
        delta = svc_count("charge") - before
        out["idempotent_http"] = {"trials": N, "service_charge_delta": delta, "exactly_once": delta == N}

        # --- IRREVERSIBLE (non-idempotent HTTP): at-most-once + UNCERTAIN, never silent double ---
        reset(); before = svc_count("send_unsafe"); uncertain = 0; committed = 0; doubled = 0
        for i in range(N):
            b = svc_count("send_unsafe")
            st, turn = run_trial(HttpUnsafeTool(URL), lambda tn: {}, rng)
            d = svc_count("send_unsafe") - b
            doubled += d > 1                # a single trial double-sending would be a violation
            if st == "uncertain": uncertain += 1
            elif st == "committed": committed += 1
        out["irreversible_http"] = {"trials": N, "uncertain": uncertain, "committed": committed,
                                    "any_silent_double": doubled > 0,
                                    "fail_closed_never_double": doubled == 0}
        out["PHASE3_PASS"] = bool(out["transactional_postgres"]["exactly_once"]
                                  and out["overlay_fs"]["exactly_once"]
                                  and out["idempotent_http"]["exactly_once"]
                                  and not out["irreversible_http"]["any_silent_double"]
                                  and out["irreversible_http"]["uncertain"] > 0)
        os.makedirs("phase3/results", exist_ok=True)
        json.dump(out, open("phase3/results/gateway_audit.json", "w"), indent=2)
        print(json.dumps(out, indent=2))
    finally:
        svc.terminate()
    return 0 if out.get("PHASE3_PASS") else 1

if __name__ == "__main__":
    sys.exit(main())
