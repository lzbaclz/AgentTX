"""Concrete tools, one per taxonomy class, for tests/gates."""
from __future__ import annotations

import os
from agenttx.gateway import Tool, ToolClass


class ChargeTool(Tool):                       # TRANSACTIONAL: exactly-once via same-tx record
    name = "charge"
    klass = ToolClass.TRANSACTIONAL

    def db_effect(self, db, key, args):
        db.execute("CREATE TABLE IF NOT EXISTS charges(order_id TEXT, amount INT)")
        db.execute("INSERT INTO charges(order_id,amount) VALUES(?,?)", (args["order"], args["amount"]))
        return f"charged:{args['order']}"


class ReceiptTool(Tool):                      # OVERLAY: content-addressed atomic FS commit
    name = "receipt"
    klass = ToolClass.OVERLAY

    def effect(self, gw, key, args):
        final = os.path.join(gw.store, "committed", f"{key}.receipt")
        if os.path.exists(final):
            return final
        tmp = os.path.join(gw.store, "overlay", f"{key}.tmp")
        with open(tmp, "w") as f:
            f.write(f"{args['order']}\n"); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, final)
        return final


class EmailTool(Tool):                        # IRREVERSIBLE: fail-closed UNCERTAIN on ambiguity
    name = "send_email"
    klass = ToolClass.IRREVERSIBLE

    def effect(self, gw, key, args):
        import uuid
        d = os.path.join(gw.store, "sent_emails"); os.makedirs(d, exist_ok=True)
        p = os.path.join(d, f"{uuid.uuid4().hex}.eml")    # non-idempotent: a new send each call
        with open(p, "w") as f:
            f.write(f"to:{args['to']}\n"); f.flush(); os.fsync(f.fileno())
        return p


import json as _json
import urllib.request as _u
from agenttx.gateway import ToolClass as _TC


class HttpIdempotentTool(Tool):       # IDEMPOTENT: pass the action key as the external idempotency key
    name = "http_charge"
    klass = _TC.IDEMPOTENT

    def __init__(self, base_url):
        self.base_url = base_url

    def effect(self, gw, key, args):
        req = _u.Request(f"{self.base_url}/charge", data=b"{}", method="POST")
        req.add_header("Idempotency-Key", key)             # action key -> external idempotency key
        return _json.load(_u.urlopen(req, timeout=5))["result"]


class HttpUnsafeTool(Tool):           # IRREVERSIBLE: a non-idempotent external API -> fail-closed
    name = "http_send_unsafe"
    klass = _TC.IRREVERSIBLE

    def __init__(self, base_url):
        self.base_url = base_url

    def effect(self, gw, key, args):
        req = _u.Request(f"{self.base_url}/send_unsafe", data=b"{}", method="POST")
        return _json.load(_u.urlopen(req, timeout=5))["result"]
