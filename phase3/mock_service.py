"""A real external HTTP service the Tool Gateway calls. Two endpoints model the two worlds:
  POST /charge      -- supports an Idempotency-Key header: a repeated key returns the cached
                       result and does NOT re-execute the effect (an idempotent external API).
  POST /send_unsafe -- NO idempotency support: every call executes (a non-idempotent
                       irreversible API, e.g. 'send an email / wire money once').
  GET  /count?kind= -- the server-side ledger count (the oracle).
The service is a separate process and stays UP across the gateway's crash+recovery (the
external world does not crash when our agent does)."""
import json, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LEDGER = {"charge": 0, "send_unsafe": 0}      # the real side-effect counters
SEEN = {}                                      # idempotency_key -> cached response
LOCK = threading.Lock()

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, obj):
        b = json.dumps(obj).encode(); self.send_response(code)
        self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(b)))
        self.end_headers(); self.wfile.write(b)
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0)); self.rfile.read(n)
        with LOCK:
            if self.path == "/charge":
                key = self.headers.get("Idempotency-Key", "")
                if key and key in SEEN:
                    return self._send(200, {"status": "dedup", "result": SEEN[key]})
                LEDGER["charge"] += 1                       # the effect (exactly once per key)
                res = f"charge#{LEDGER['charge']}"
                if key: SEEN[key] = res
                return self._send(200, {"status": "ok", "result": res})
            if self.path == "/send_unsafe":
                LEDGER["send_unsafe"] += 1                  # NO dedup -> a retry double-sends
                return self._send(200, {"status": "ok", "result": f"sent#{LEDGER['send_unsafe']}"})
        self._send(404, {})
    def do_GET(self):
        if self.path.startswith("/count"):
            kind = self.path.split("kind=")[-1]
            with LOCK: return self._send(200, {"count": LEDGER.get(kind, 0)})
        self._send(404, {})

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8731
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
