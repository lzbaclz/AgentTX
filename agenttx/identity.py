"""Single source of action-identity semantics — shared by the single-owner Gateway
(`agenttx/gateway.py`) and the distributed protocol (`agenttx/dtx.py`) so the two can never
diverge. (The advisor flagged that they previously used two different identity schemes, and the
rigorously crash-tested one was not the one the taxonomy described.)

ACTION IDENTITY IS POSITIONAL: ``(session, turn, model_output_commit_id, action_ordinal)``.

  * ``commit_id`` = a hash of the committed model output that decided this turn's plan; binds the
    plan to the model output that produced it (a re-decided plan is a different turn-of-actions).
  * ``ordinal``   = the action's position in that plan. Two LEGITIMATELY identical tool calls in
    one turn (e.g. two $100 charges) have different ordinals -> different ids -> BOTH execute.

The args fingerprint is a CONTENT CHECK ONLY, never a dedup key: if a dedup hit's stored
fingerprint disagrees with the replayed args, that is corruption / non-determinism, and we raise
``ContentMismatch`` rather than silently returning the cached result.

Hashes are FULL sha256 (64 hex). No truncation: a truncated key trades a vanishing-but-nonzero
collision probability for a SILENT FALSE DEDUP that would DROP a real effect -- unacceptable for an
exactly-once claim. (The old code used 64/80/96-bit truncations in three different files.)
"""
from __future__ import annotations

import hashlib
import json


class ContentMismatch(Exception):
    """A replayed action's args disagree with the fingerprint recorded under its action id."""


def canonical_args(args) -> str:
    return json.dumps(args, sort_keys=True, default=str)


def action_id(session, turn, commit_id, ordinal) -> str:
    return hashlib.sha256(f"{session}|{turn}|{commit_id}|{ordinal}".encode()).hexdigest()


def args_fingerprint(args) -> str:
    return hashlib.sha256(canonical_args(args).encode()).hexdigest()


def commit_id_of(plan) -> str:
    """Stand-in for the hash of the committed model output that produced this plan.
    Stable under re-serialization (canonical JSON)."""
    return hashlib.sha256(canonical_args(plan).encode()).hexdigest()
