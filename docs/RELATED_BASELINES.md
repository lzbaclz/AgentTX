# Baselines & SOTA positioning

The advisor correctly insisted the strong baseline is DBOS **with** its recommended idempotency,
not naked DBOS. This is the honest head-to-head + capability matrix (`phase10/sota_matrix.py`).

## Measured head-to-head (real systems, same non-transactional-effect crash workload)
| system | effect under crash+replay | exactly-once? | role |
|---|---|---|---|
| none / checkpoint (naive re-run) | duplicate | ✗ | naive floor |
| **real DBOS, naked `@DBOS.step`** | 2 files | ✗ | *failure example only* — not the strong baseline |
| **real DBOS + idempotent (content-addressed) effect** | 1 file | ✓ | **STRONG baseline — matches AgentTx (1/1)** |
| **real LangGraph + PostgresSaver** | charges 2 / receipts 2 | ✗ | no effect transaction |
| **AgentTx** | 1 | ✓ | phase3 (300/class) + phase7 (distributed) |

**Takeaway:** for a *well-behaved* (idempotent / transactional) effect, a careful DBOS user already
gets exactly-once. AgentTx does **not** claim to beat that. Our contribution is the part no baseline
covers.

## Capability matrix across recovery planes
(`yes/no/partial/n/a`; `(m)` = measured in this repo; paper rows are from the cited works, not re-run)

| system | effect-XO (well-behaved) | mid-effect non-atomic | distributed concurrent recovery | KV plane | durable output | workflow |
|---|---|---|---|---|---|---|
| DBOS + idempotency | yes(m) | no | no | no | no | yes |
| DBOS + transactional outbox | yes | yes | no | no | no | yes |
| Temporal + idempotent activities | yes | partial | no | no | no | yes |
| LangGraph + PostgresSaver | no(m) | no | no | no | no | yes |
| Atomix (2602.14849) | yes | yes | no (single-process) | no | no | partial |
| Cordon (2606.17573) | yes | yes | no | no | no | partial |
| Concordia (2606.23521) | n/a | n/a | n/a | yes | no | no |
| Crab / DeltaBox | n/a | n/a | n/a | partial (sandbox) | no | no |
| **AgentTx (this work)** | **yes(m)** | **yes(m)** | **yes(m)** | view* | **yes(m)** | **yes(m)** |

`view*` = byte-exact, fail-closed, content-addressed KV CAS that survives hard process death
(phase2 + phase8); injecting it into a *fresh vLLM engine's attention* to resume decoding is TARGET.

## What is genuinely new (no single system fills these columns together)
1. **Distributed concurrent recovery** — owner-epoch fencing + atomic action claim. DBOS/Temporal
   recover a workflow under a single owner; Atomix/Cordon are single-process. AgentTx is gated under
   real multi-process concurrent recovery (`phase7`).
2. **One cross-plane turn-prefix contract** — effect + KV + durable output + workflow recover to the
   *same* committed turn boundary. Each baseline hardens one plane and assumes the others away.
3. **Mid-effect crashes on non-atomic effects** — the τ²-bench result (`phase6`): the per-tool guard
   that protects against full replay does not protect against a crash mid-effect; AgentTx's
   transactional wrap does.

## Not re-run here (honest)
Temporal (needs a server), Atomix (`github.com/mpi-dsg/atomix`), and Cordon are described from their
papers, not executed in this repo. Running them under our exact fault harness is camera-ready work;
their *mechanisms* (idempotent activities / frontier-gated commit / semantic transactions) are
effect-plane techniques that AgentTx can compose as backends, not compete with on the effect plane.
