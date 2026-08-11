# Workload Manager Memory Limits — Benchmark Report and Decision Verdicts

Status: measurement report, 2026-08-09. Companion to `workload_manager_memory_limits_plan.md` (decisions D1–D12, Steps 1–12) and `workload_manager_memory_limits_design.md`.

Question answered: **how much would an MVP implementation of the proposed decision defaults degrade latency, memory consumption, and scalability versus current trunk** — data to defend the defaults or send them back for deeper discussion.

## 1\. What was measured

### 1.1 Baseline

Upstream trunk `bb228c7f4c0` (2026-08-09), unmodified. Bench suite and prototype patches are committed on branch `bench/memory-baseline` (draft PR: <https://github.com/lberserq/ydb/pull/3>); the RM prototype patches themselves are never applied in committed form — they live as `bench_patches/*.patch` files.

### 1.2 MVP of the decision defaults

A 146-line prototype patch to `ydb/core/kqp/rm_service/kqp_rm_service.cpp` (`bench_patches/mvp_rm.patch` on branch `bench/memory-baseline`, paste: <https://paste.yandex-team.ru/c2a7f06c-eabb-4466-8af1-8183a22daaf8/text>) approximating the hot-path cost of the plan's defaults:

  - **4-level accounting** (D3/D10, Step 5): a database-level `TMemoryResource` account acquired/released under the existing RM lock in addition to node + pool — node → database → pool → query.
  - **Per-query limit check** (D3, Step 6): one atomic load + compare against a query limit derived from the pool limit on every allocation.
  - **ExternalMemory charged to the pool** (D6, Step 4): the currently lock-free ExternalMemory path (two atomics) additionally takes the RM lock and acquires/releases pool + database accounts.
  - **Subscription-owned pool entries** (Step 4 / 1c): no erase-on-zero; pool config not rewritten from tx state (`SetNewLimit` removed from the tx path).

Denial semantics unchanged; limits set so nothing is denied in the benches — only the accounting overhead is measured.

### 1.3 Benchmarks

`KqpRmBench` unit-test suite (`ydb/core/kqp/rm_service/kqp_rm_bench_ut.cpp` on branch `bench/memory-baseline`), real `TKqpResourceManager` + ResourceBroker in a test actor runtime:

| Bench                                         | Path exercised                                                                                              |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `rm_alloc_free_no_pool`                       | Alloc+free 1 MiB, no resource pool — **control**: code identical in both binaries                           |
| `rm_alloc_free_named_pool`                    | Same, tx bound to a named pool (50%)                                                                        |
| `rm_alloc_free_external_only`                 | ExternalMemory-only reservation (initial task/channel reservations path)                                    |
| `rm_oltp_query_cycle`                         | Full RM lifecycle of a small query: tx create → 1×(ExecutionUnits+ExternalMemory) alloc → free → tx destroy |
| `rm_alloc_free_named_pool_contended{4,16,32}` | N threads, each its own tx, same pool — lock scalability                                                    |
| `mkql_churn_{plain,callback}_limit`           | MKQL page-pool mixed-size churn: latency + fragmentation, huge limit vs tight limit with granting callback  |

### 1.4 Protocol

  - Interleaved A/B: base and MVP binaries alternate per test per round — machine-load drift cancels in the comparison.
  - Final dataset (v3): **20 rounds × 100 in-process iterations × 2000 ops/iteration** → 2000 samples per (test, variant); medians of per-invocation percentiles across rounds.
  - Host: 57 cores, load ≈ 1.3 at start, ≈ 4 during (the benches themselves); per-round load stamps recorded in the raw log.
  - Control-path noise floor: **p50 +1.9%, p95 +0.4%, p99 −0.6%** — deltas under \~2% are noise.
  - Raw logs: `baseline_bench.log`, `ab_bench_v2.log`, `ab_bench_v3.log`, aggregate `v3_agg.json` (session scratchpad).

## 2\. Results (v3, final)

Alloc+free pair unless stated; ns per operation.

| Bench               | base p50 | mvp p50 | Δp50      | Δp95   | Δp99               |
| ------------------- | -------- | ------- | --------- | ------ | ------------------ |
| no pool (control)   | 1849     | 1883    | \+1.9%    | \+0.4% | −0.6%              |
| named pool          | 2023     | 2146    | **+6.1%** | \+4.4% | \+1.6%             |
| ExternalMemory-only | 54       | 213     | **+294%** | \+268% | \+269%             |
| OLTP query cycle    | 207      | 366     | **+77%**  | \+73%  | \+71% (p99 389 ns) |
| contended ×4        | 27 824   | 27 770  | −0.2%     | \+0.1% | \+1.5%             |
| contended ×16       | 138 841  | 141 693 | \+2.1%    | \+2.6% | \+2.8%             |
| contended ×32       | 287 025  | 284 619 | −0.8%     | −0.6%  | −0.7%              |

MKQL page pool (unchanged by the MVP): \~120 ns/op churn latency; the limit-increase callback adds \~5%; after freeing half the blocks the pool retains all pages (retained/used ≈ 2×) until shrink/`ReleaseFreePages` — the existing 30 MiB shrink hysteresis, not the arbiter, is the fragmentation control.

## 3\. Findings

### 3.1 The accounting cost of the defaults is noise on the growth path

Named-pool alloc+free gains \~123 ns (+6.1%) for query-check + database account. This path runs once per quota chunk — 1 MiB alignment, 30 MiB minimum (`MinMemAllocSize`) — so per 30 MiB of real memory growth the added cost is \~0.1 µs. Invisible at any percentile of query latency.

### 3.2 ExternalMemory charging (D6) is the one structurally slower path

54 → 213 ns (\~4×): a lock-free two-atomic path becomes lock + two map lookups + two acquires (×2 for free). Absolute cost +159 ns per reservation, at task-start / 1 MiB-channel-step frequency.

**Resolved by the lock-free variant (validated 2026-08-09, `bench_patches/d6_lockfree.patch`, paste: <https://paste.yandex-team.ru/3fea8b2d-c10e-451b-b0ad-c9d2585ce131/text>, 10-round 3-way interleaved run):** pool account handle resolved once per tx (cached in `TTxState`), per-op charge is one relaxed `fetch_add` on the pool account; db/node aggregates derive at publish cadence. Measured: external path **58 ns** (base 54, locked MVP 215); OLTP cycle **259 ns** p50 / 328 ns p99 (base 206/227, MVP 368/383) — the residual +53 ns is the once-per-query handle resolution. External charging no longer touches the RM lock at all.

### 3.3 OLTP: per-query RM cost doubles — to 366 ns

The full RM lifecycle of an OLTP-shaped query goes 207 → 366 ns p50 (p99 389 ns). Against even a 1 ms E2E budget that is 0.04%; against 10 ms, 0.004%. An OLTP query with a light program touches RM exactly once and never enters the incremental-growth path. **Per-query RM cost is not an OLTP risk.**

### 3.4 The real issue is pre-existing: the RM lock path does not scale

Contended per-op latency grows linearly with threads in **both** variants (base ≈ MVP within noise):

| Threads | per-op p50 | aggregate throughput |
| ------- | ---------- | -------------------- |
| 1       | \~2 µs     | \~500 kops/s         |
| 4       | \~28 µs    | \~144 kops/s         |
| 16      | \~139 µs   | \~115 kops/s         |
| 32      | \~287 µs   | \~111 kops/s         |

Aggregate throughput *collapses* \~4.5× from 1 → 32 threads (lock convoy + cache-line ping-pong on the RM lock + ResourceBroker-instant calls). The MVP adds nothing measurable to a saturated lock. Today this is masked by 30 MiB chunking; the bound stays comfortable — even at full saturation one OLTP query's single RM cycle costs \~0.3 ms — but the batching is load-bearing.

## 4\. Verdicts on the decision defaults

| Decision                                                      | Verdict                                                                                                                                                        | Evidence |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| D3 query-level check (pool-relative limit)                    | **Defend** — one atomic check, inside the +6.1% named-pool delta                                                                                               | §3.1     |
| D10 database level in the account tree                        | **Defend** — same                                                                                                                                              | §3.1     |
| D6 ExternalMemory charged to pool                             | **Defend, with recorded caveat** — 4× relative but +159 ns absolute at low frequency; name the lock-free per-pool-atomics escape hatch in the decision record  | §3.2     |
| Step 5 single-lock arbiter                                    | **Defend, with a new gate** — acceptable only together with ≥1 MiB batching; add a no-regression gate on the §3.4 scaling curve to Step 5's definition of done | §3.4     |
| Step 4 subscription-owned pool entries (no erase-on-zero)     | **Defend** — removes per-op entry churn; part of the measured MVP                                                                                              | §1.2     |
| Quota chunk sizes (`MinMemAllocSize` 30 MiB, 1 MiB alignment) | **Do not reduce without re-benching** — they are what keeps §3.4 harmless                                                                                      | §3.4     |

Follow-up added to the plan: if RM call frequency rises (smaller chunks, Step 7 coverage expansion), per-pool lock sharding or relaxed per-pool atomics must land first, starting with D6's external charging.

## 5\. Reproduction

<div id="cb1" class="sourceCode">

``` sourceCode bash
# worktree .claude/worktrees/membench at trunk bb228c7f4c0
./ya make --build relwithdebinfo ydb/core/kqp/rm_service/ut   # baseline
# apply mvp_rm.patch, rebuild -> MVP binary
# run: <ut-binary> "KqpRmBench::<TestName>", parse BENCHSUMMARY/BENCHFRAG
# interleave base/mvp per test per round; >= 20 rounds recommended
```

</div>

Bench source: `ydb/core/kqp/rm_service/kqp_rm_bench_ut.cpp` (+ 1 line in `ut/ya.make`); patches in `ydb/core/kqp/rm_service/bench_patches/` — all on branch `bench/memory-baseline` (draft PR <https://github.com/lberserq/ydb/pull/3>, measurement-only, not for merge). The real implementation follows the plan's Steps 1–6 with tests first.
