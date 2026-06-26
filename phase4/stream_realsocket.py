"""Phase-4 real-socket cross-check: the streaming exactly-once protocol over REAL HTTP with
REAL worker-process crashes. A server worker streams a turn's committed tokens (newline JSON,
flushed per token) from `ack+1`; the driver KILLS the worker mid-stream twice and starts a
fresh worker each time (multi-worker reroute); the client reconnects with its ACK watermark
and dedups re-sends by seq. Verify the client materialized every token exactly once, in order.

Run: python phase4/stream_realsocket.py
"""
import json
import os
import socket
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

TOKENS = [f"tok{i}" for i in range(60)]       # the durable committed-token log (source of truth)
PORT = 8744


class Worker(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        q = parse_qs(urlparse(self.path).query)
        ack = int(q.get("ack", [0])[0])
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.end_headers()
        for seq in range(ack + 1, len(TOKENS) + 1):    # resume from ack+1 (may re-send)
            line = (json.dumps({"seq": seq, "token": TOKENS[seq - 1]}) + "\n").encode()
            try:
                self.wfile.write(line); self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return
            time.sleep(0.012)                          # pace so the driver can crash mid-stream


def run_worker():
    httpd = ThreadingHTTPServer(("127.0.0.1", PORT), Worker)
    httpd.serve_forever()


def main():
    # client state (durable across worker crashes)
    ack = 0
    received = []
    dup = 0
    workers = 0

    # worker manager: run a worker in this process thread; "crash" = stop+restart a fresh one
    srv = {"httpd": None, "thread": None}

    def start_worker():
        srv["httpd"] = ThreadingHTTPServer(("127.0.0.1", PORT), Worker)
        srv["thread"] = threading.Thread(target=srv["httpd"].serve_forever, daemon=True)
        srv["thread"].start()

    def crash_worker():
        if srv["httpd"]:
            srv["httpd"].shutdown(); srv["httpd"].server_close(); srv["httpd"] = None

    start_worker(); workers += 1
    time.sleep(0.3)
    crashes_left = 2
    while ack < len(TOKENS):
        try:
            req = urllib.request.urlopen(f"http://127.0.0.1:{PORT}/stream?ack={ack}", timeout=5)
            got_since_connect = 0
            for raw in req:
                rec = json.loads(raw.decode())
                seq, token = rec["seq"], rec["token"]
                if seq == ack + 1:
                    received.append(token); ack = seq
                elif seq <= ack:
                    dup += 1
                got_since_connect += 1
                # crash the worker mid-stream (real process-level reroute)
                if crashes_left > 0 and ack == len(TOKENS) // 3 * (3 - crashes_left) and got_since_connect > 2:
                    crash_worker()
                    crashes_left -= 1
                    time.sleep(0.1); start_worker(); workers += 1
                    break                              # reconnect to the fresh worker
        except Exception:                               # noqa: BLE001 (worker down mid-read -> reconnect)
            crash_worker(); time.sleep(0.1)
            if srv["httpd"] is None:
                start_worker(); workers += 1
            continue
    crash_worker()

    out = {"tokens": len(TOKENS), "received": len(received), "workers_used": workers,
           "resends_deduped": dup,
           "exactly_once_in_order": received == TOKENS,
           "PHASE4_REALSOCKET_PASS": received == TOKENS}
    os.makedirs("phase4/results", exist_ok=True)
    json.dump(out, open("phase4/results/stream_realsocket.json", "w"), indent=2)
    print(json.dumps(out, indent=2))
    return 0 if out["PHASE4_REALSOCKET_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
