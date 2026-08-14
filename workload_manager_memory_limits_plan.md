# Workload Manager Memory Limits — Detailed Implementation Plan

Status: detailed plan, based on `workload_manager_memory_limits_design.md` (design draft) and a code audit of the current trunk (2026-07-25). All file:line references verified.

## 1\. Code Audit Results

### 1.1 Design-doc claims — all four gaps confirmed

| Gap                                             | Verdict   | Anchor                                                                                                                                                                                                     |
| ----------------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Zero pool limit bypasses named-pool accounting  | Confirmed | `ydb/core/kqp/rm_service/kqp_rm_service.cpp:295` — guard `tx.MemoryPoolPercent > 0`; 0 means "no pool enforcement", not "deny all"                                                                         |
| Stale config: old tx restores pre-ALTER percent | Confirmed | `kqp_rm_service.cpp:301` — `SetNewLimit()` called with `const` `TTxState::MemoryPoolPercent` captured at tx start (`kqp_rm_service.h:57`); no version stamp on the shared pool                             |
| ExternalMemory not charged to named pool        | Confirmed | `kqp_rm_service.cpp:258-264` — early return when `resources.Memory == 0`; initial per-task reservation from node service comes as `ExternalMemory` with `Memory=0` (`kqp_query_control_plane.cpp:323-329`) |
| No focused named-pool tests                     | Confirmed | `kqp_rm_service/kqp_rm_ut.cpp:198` — `MakeTx()` always uses empty PoolId / 100%; `MemoryNamedPools` path has zero test coverage                                                                            |

### 1.2 The commented-out throw in `dq_compute_memory_quota.h` — investigated

`dq_compute_memory_quota.h:143-147`: on `AllocateQuota()` denial only a warning is logged; the `throw yexception()` below is commented out.

**History (traced to the origin in internal Arcadia).** The line was born commented — it was never live code:

  - Origin: arc commit `b825db9aba3e` (svn r6988105, imakunin, 2020-06-19), KIKIMR-9865 "дозапрашивать память для mkql программ в процессе выполнения", review <https://a.yandex-team.ru/review/1304556>, then in `kikimr/core/kqp/compute_actor/kqp_compute_actor_impl.h`.
  - The same commit introduced `SetIncreaseMemoryLimitCallback` + `TryIncreaseLimit` in `yql/minikql/aligned_page_pool.*`, rewriting the pre-existing typed throw sites to "over limit → try callback → still over → throw `TMemoryLimitExceededException`". The callback contract from day one: denial is signaled by *not raising the limit*; the pool throws.
  - Review 1304556 also records the status-semantics debate relevant to D4: spuchin argued the denial is `PRECONDITION_FAILED`; imakunin kept it an execution error ("depending on data placement the query may or may not succeed") — today's `OVERLOADED`.
  - The KIKIMR-9865 ticket confirms intent: the author called the change a prototype ("этот код скорее прототип") and vvvv approved the synchronous-callback design ("да понятно, синхронный callback. Нормально"). The commented throw is residue of that prototype stage.
  - The original feature also had a blocking wait mode (`EnableInstantMkqlMemoryAlloc`, wait up to `InstantMkqlMemoryAllocWaitTimeMs` =20ms polling every 1ms, then MemoryLimitExceeded). It was later removed — fields 8-10 are `reserved` in `ydb/core/protos/table_service_config.proto:36-38`. Today denial is instant. If D5 (spill-before-kill grace) reintroduces waiting, it must be asynchronous, not the old poll-loop (design draft: executor threads never block on memory).
  - The code reached the public repo already commented with the Feb 2022 import (`1110808a9d3`) and was moved verbatim into `dq_compute_memory_quota.h` by `d4d446c9c53` (YQ-833, Apr 2022).

**Why it is safe (and correct) that it stays commented.** Enforcement lives one level below. `RequestExtraMemory` is invoked from `TAlignedPagePool::TryIncreaseLimit()` (`yql/essentials/minikql/aligned_page_pool.cpp:685-691`), which after the callback checks `Limit_ >= required`; when the quota was denied the limit was not raised, `TryIncreaseLimit` returns false, and the page pool throws the typed `TMemoryLimitExceededException` (`aligned_page_pool.cpp:400,448,546`). Compute actors catch exactly this type and fail the query with `OVERLOADED` / "Mkql memory limit exceeded". A generic `yexception` thrown from inside the callback would *bypass* that classification and surface as INTERNAL\_ERROR — strictly worse. Existing tests `KqpLimits::ComputeActorMemoryAllocationFailure[QueryService]` (`ydb/core/kqp/ut/query/kqp_limits_ut.cpp:291,315`) verify the denial → typed exception → `OVERLOADED` path end-to-end for the node-wide limit; both were re-run on current trunk (2026-07-25) and pass ("Mkql memory limit exceeded", status OVERLOADED, code 2029).

**Consequences for the plan:**

  - Do NOT uncomment the throw — delete the dead commented code instead, with a comment pointing at the page-pool enforcement (small cleanup PR).
  - Denial enforcement for compute pages works today; the real gaps are *what feeds the denial* (named pools miss ExternalMemory, zero-percent bypass, stale config) and *what is not routed through the quota at all* (see 1.3.6).
  - Any change to denial semantics (e.g. adding spill-retry grace before failing, or making named-pool denial stricter) is a behavior change and MUST be introduced behind a feature flag, default off, with observe-only counters first. Nothing in this area may be changed by "just uncommenting".
  - Remaining subtlety worth a test: `UpdateMemoryYellowZone()` is suppressed while an `IncreaseMemoryLimitCallback_` is set and the max-limit flag is not reached (`aligned_page_pool.cpp:672`), so spilling pressure for quota-managed allocs comes only from `IsReasonableToUseSpilling()` (RM `SpillingPercent` cookies), not from the pool-local yellow zone.

### 1.3 Other findings not in the design draft

1.  **`QUERY_MEMORY_LIMIT_PERCENT_PER_NODE` is rejected at validation.** `ydb/core/resource_pools/resource_pool_settings.cpp:74-76` returns "not supported yet" for any value \!= -1. Enabling per-query limits requires lifting this validation plus a rolling-upgrade compatibility story.

2.  **Memory Controller has no KQP consumer.** `EMemoryConsumerKind` (`ydb/core/base/memory_controller_iface.h:7-18`) contains cache/memtable/CS consumers only. The query-execution budget is delivered as a Resource Broker queue limit: `GetQueryExecutionLimitBytes()` → `TEvResourceBroker::TEvConfigure` on `KqpResourceManagerQueue` (`memory_controller.cpp:446-501`); consumption is observed as `TAlignedPagePool::GetGlobalPagePoolSize()`. The arbiter's node root limit should track the RB queue limit exactly as `TotalMemoryResource` already does via `TEvConfigResponse` (`kqp_rm_service.cpp:768`). No new Memory Controller consumer is needed for steps 1–6.

3.  **Per-pool memory is completely invisible in monitoring.** `MemoryNamedPools` is not exported anywhere: the RM mon page shows node-wide totals only (`kqp_rm_service.cpp:875-922`), whiteboard publishes one legacy entry. Observe-only rollout is impossible until counters exist.

4.  **The CPU scheduler tree is directly extensible, but has lifetime gaps.** `TStaticAttributes` (`runtime/scheduler/tree/common.h:33-38`) holds only `Weight/CpuLimit/CpuGuarantee/ReadLimit`. Pool config reaches the scheduler via a fully reusable path: pool handler SchemeCache watch → `NWorkload::TEvUpdatePoolInfo` → `TComputeSchedulerService` (`kqp_compute_scheduler_service.cpp:140-186`). However `TEvRemoveDatabase`/`TEvRemovePool` are `Y_ABORT("Unsupported yet")` (`kqp_compute_scheduler_service.cpp:100-101,136-137`) — memory account lifetime must not depend on tree removal until this is implemented.

5.  **The managed-allocation hook already exists and is batched.** `IMemoryQuotaManager` (`dq_compute_actor_async_io.h:39`) is the single synchronous gate for compute-heap growth; requests are aligned to 1 MiB with a 30 MiB minimum chunk (`MinMemAllocSize`) and 30 MiB shrink hysteresis. Channel buffers go through a second instance (`TChannelQuotaManager`, `kqp_query_control_plane.cpp:78`). This batching bounds arbiter hot-path contention by design — a locked hierarchy update per ≥1 MiB request is acceptable; no lock-free tree is needed initially.

6.  **Coverage holes in today's quota.** Covered: MKQL pages, Arrow via `OffloadAlloc` (when `EnableArrowTracking`), channel buffers. Not covered: `UseDefaultArrowAllocator()` path, stream-lookup iterator buffers (`MaxTotalBytesQuotaStreamLookup`-style ad-hoc limits), write-actor in-flight buffers (`InFlightMemoryLimitPerActorBytes`), and all `ExternalMemory` initial reservations at named-pool level.

7.  **tcmalloc tag plumbing is complete end-to-end and cheap to extend.** TLS tag (`ydb/library/actors/prof/tcmalloc.cpp`) → sampled allocation `user_data` via `SetSampleUserDataCallbacks` → per-tag sampled counters with `MaxTag = 2048` (`ydb/core/mon_alloc/tcmalloc.cpp:672-694`). The executor sets the tag to the actor *activity type* only, on activity change (`executor_thread.cpp:272-277`). A composite (activity, pool) tag fits the 2048 budget if pool tags are interned; per-query tags stay out (unbounded cardinality). Sampling only — never for enforcement.

8.  **Spilling signal path is in place and pool-aware.** `TTxState::IsReasonableToStartSpilling()` (`kqp_rm_service.h:88-91`) reads both node and pool spilling cookies; the flag flows to `TAlignedPagePool::SetMaximumLimitValueReached()` after every extra-quota request. `SpillingPercent` is node-global; per-pool thresholds would be new behavior.

9.  **`TCpuQuotaManager` is not a reusable model** (confirms the draft): it is a cluster-level EMA admission gate for `DATABASE_LOAD_CPU_THRESHOLD`, not a fair-share mechanism.

### 1.4 Refined architecture decision

The draft's "memory arbiter adjacent to the CPU scheduler" holds, refined:

  - **Reuse as-is:** identity types (`TDatabaseId/TPoolId/TQueryId`, `runtime/scheduler/fwd.h`), the pool-config subscription flow (`TEvSubscribeOnPoolChanges` / `TEvUpdatePoolInfo`), and the `TEvAddQuery`/`TEvRemoveQuery` lifecycle pattern.
  - **Do not reuse:** snapshot fair-share math, delay computation, the CAS-on-`CpuUsage` gate — memory is occupancy, not rate.
  - **Location:** new library `ydb/core/kqp/rm_service/memory_arbiter/` consumed by `TKqpResourceManager`; promoted to a standalone service only when actor-context steps need it outside KQP.
  - The generic `NActors::NMemory` interfaces from the draft appear only in Step 8, when the actor runtime needs them; earlier steps stay KQP-internal.

### 1.5 Prior-art survey — adopted ideas

Ten systems surveyed in `workload_manager_memory_prior_art.md` (ClickHouse, CockroachDB, TiDB, Velox, DuckDB, DataFusion, Trino/Presto, Impala, SQL Server, Greenplum). Convergent findings that change this plan:

  - **First-toucher denial at node scope is an outlier design.** Every surveyed system resolves node-level pressure by victim selection (most-over-guarantee or largest consumer), bounded waiting, or arbitration — not by failing whichever query allocates next. YDB is currently first-toucher at both pool and node scope. → recommendations R1/R2.
  - **Guarantee + limit per pool** (ClickHouse overcommit denominator, Greenplum fixed+shared tiers, SQL Server MIN/MAX\_MEMORY\_PERCENT) is the standard data model that makes victim ranking (`used / guarantee`) well-defined. → R1, folded into Step 5/11.
  - **Escalating action chain on denial** (TiDB: throttle → spill → cancel; Trino blocked state; SQL Server resource semaphore): spill and shrink first, bounded async wait second, typed failure last. → R3, folded into Step 6.
  - **Admission gates on reservations, not consumption** (Impala; Trino softMemoryLimit queues new queries and never kills running ones). → R4, folded into Step 11.
  - **Grant estimation + percentile feedback** (SQL Server adaptive grants). → R5, new follow-up ticket.
  - **Arbitration as the end-state** (Velox SharedArbitrator: strip free quota → command spills by reclaimable bytes → abort largest): design Step 5's interfaces so reclaim can be added without breaking changes. → R6.
  - Cheap diagnostics adopted early: top-consumers annotation on every denial error (DataFusion TrackConsumersPool), pre-OOM pressure dumps and a kill/deny audit history (TiDB). → R9, into Steps 2 and 9.
  - Anti-patterns to avoid, with upstream receipts: Presto's reserved pool (removed as a design mistake, trino\#6677); memory-aware throttling of *running* work as an admission substitute (CockroachDB admission notes: it can increase memory held).

### 1.6 Recent research (arXiv)

Moved to `workload_manager_memory_prior_art.md` §14 (SafeLoad admission control, buffer-management survey, NVM-as-primary-storage — comparison and adopted ideas). Ticket \#18 in section 5 tracks the SafeLoad-style follow-up.

### 1.7 Internal zone-model design (hor911, sbr://13145352241) — alignment

The internal presentation "О самом дорогом — памяти" defines the target
maturity ladder and a per-level control model this plan must align with:

- **Maturity ladder:** no limits → hard limits (deny) → **yellow zone**
  (spill; "we are here" — the `SpillingPercent` cookie path, pinned by
  Step 1 tests) → **red zone** (asynchronous memory release/eviction) →
  minimum limits + planned execution.
- **PUSH/PULL model per level:** a container PUSHes limits (and its
  green/yellow/red thresholds) down to components; a component PULLs limit
  changes from its container. Mapping: Memory Controller — N/A; Resource
  Broker — PUSH; KQP RM — PULL; **WM Resource Pool — PUSH(?); Query —
  PUSH(?)**; Task — PULL. This plan resolves the two question marks: pool and
  query accounts receive PUSHed limits from subscription-owned config
  (Step 4) and the arbiter (Step 5), while tasks keep PULLing via
  `IMemoryQuotaManager`.
- **Zone semantics adopted into the arbiter (Step 5):** every account level
  carries green/yellow/red thresholds. Yellow is checked synchronously at
  limit-increase (PULL) time and denies "optional" growth — this is today's
  `IsReasonableToStartSpilling` generalized per account. Red triggers
  asynchronous reclaim — the component returns memory "when it can, as much
  as it can" — which is exactly the D12/R6 reclaim contract; red-zone
  semantics are now the concrete specification of that contract.
- **Upstream dependencies owned by the hor911 track** (this plan consumes
  them, does not implement them): the RB→RM traffic light (zone flags pushed
  from Resource Broker), and the Memory Controller fix to distribute >100%
  (overcommit) in any configuration. Tracked as coordination follow-ups.

**Sequencing (agreed):** per-node pool limitation lands first — Steps 1–6
are strictly node-local by design. Cluster-wide pool/query limitation comes
after, via zverevgeny's proposal: the workload manager consumes
**gossip-distributed** per-pool usage (the existing
`TKqpResourceInfoExchanger` already gossips node resource snapshots —
`ResourceSnapshotState` in `kqp_rm_service.cpp`). Eventually-consistent
aggregates are inaccurate for short queries (staleness window ≈ exchanger
period) but adequate for the long-running queries that dominate memory —
short queries remain governed by node-local limits only. See Step 13 and
D13.

### 1.8 Node-wide memory governance (separate design doc)

The scope above covers the QueryExecution category only. The follow-on
rework — Workload Manager as the memory *policy* source for ALL Memory
Controller categories (shared cache, MemTable, compaction, backup/restore),
with a demand-driven rebalancing protocol between KQP and shards mediated
by the MC cycle — is specified in `workload_manager_memory_governance.md`
(D14–D16, Steps 14–16). It reuses this plan's account model (D10
guarantee/limit, D12 reclaim contract, §1.7 zones) one level up and
subsumes the hor911 TODOs ("RB traffic light", "MC >100%").

## 2\. Decisions Required Before Coding (Step 0)

| \#  | Decision                                                                                                                                 | Blocks    | Proposed default                                                                                                                               |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| D1  | Percentage base: node limit = RB `KqpResourceManagerQueue` limit (Memory Controller budget)                                              | Step 4    | Yes — matches current `TotalMemoryResource` (OQ1)                                                                                              |
| D2  | Zero pool limit semantics: reject at admission (workload service), not at first allocation                                               | Step 4    | Admission-time `PRECONDITION_FAILED` (OQ3)                                                                                                     |
| D3  | Query limit base: percent of *effective pool limit*, not raw node                                                                        | Step 6    | Pool-relative (OQ2)                                                                                                                            |
| D4  | Denial statuses: `OVERLOADED` for quota denial (matches today), `PRECONDITION_FAILED` for hard per-query limit                           | Step 4/6  | As stated (OQ4)                                                                                                                                |
| D5  | Spill-before-kill budget for over-limit queries — **total per episode** across spill→shrink→wait, stage timeouts derived from it; D11's wait gets the remainder | Step 6    | Bounded retries, \~10s total default (OQ5)                                                                                                     |
| D6  | ExternalMemory charged to the pool                                                                                                       | Step 4    | Yes, via the lock-free single-atomic design in Step 4 (per-tx account handle + relaxed pool atomic; db/node derived at publish cadence) (OQ18) |
| D7  | Weight semantics: reuse `RESOURCE_WEIGHT` for memory vs. new setting                                                                     | Step 11   | Defer; document current "CPU only" (OQ9/10). Until resolved, guarantee = limit everywhere and `used/guarantee` ranking degenerates to largest-consumer — D10's floor has no effect in default configs |
| D8  | Default pool: memory limits allowed?                                                                                                     | Step 4    | Yes, same as any pool (OQ7)                                                                                                                    |
| D9  | Rolling-upgrade behavior for accepting `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE`                                                             | Step 6    | Feature flag + old nodes ignore (OQ44)                                                                                                         |
| D10 | Pool memory *guarantee* alongside the limit (basis for victim ranking `used/guarantee`; sum of guarantees ≤ node budget ≤ sum of limits) | Step 5/11 | New optional setting, default = limit (no overcommit) — prior-art R1. **Assignment**: `MEMORY_GUARANTEE_PERCENT_PER_NODE` schema/proto/DDL added in Step 5; floor-protection trigger enforced in Step 11; D7 enables auto-computation |
| D11 | Bounded async wait-for-quota before terminal denial (memory waiters woken by `FreeQuota`)                                                | Step 6    | Feature flag, default off; timeout \~ D5 budget — prior-art R3                                                                                 |
| D12 | Arbiter API reserves the reclaim contract (`ReclaimableBytes` in snapshots, async "reclaim N" event) even if unimplemented initially     | Step 5    | Yes — avoids breaking interface change later — prior-art R6                                                                                    |
| D13 | Cluster-wide pool/query limits: eventually-consistent enforcement over gossip (`TKqpResourceInfoExchanger`) — staleness bound = exchange period; short queries governed by node-local limits only | Step 13 | Yes (zverevgeny's proposal); no synchronous cluster coordination on the allocation path |

Deliverable: decisions recorded in the Tracker ticket; design doc updated.

## 3\. Step-by-Step Plan

Tests-first ordering: every behavior change is preceded by tests that pin the current behavior, and every enforcement change ships behind a feature flag, default off.

### Step 1 — Write the missing tests (pin current behavior)

No production code changes. Each test documents today's semantics; tests that expose a gap assert the *current* (buggy) behavior with a comment referencing the fix step, and are flipped by that step's PR.

1a. `kqp_rm_service/kqp_rm_ut.cpp` — add pool-aware `MakeTx` overload; tests:

  - two named pools on one node, isolation between them;
  - same pool name in two databases (key is `(database, pool)`);
  - zero-percent pool → currently bypasses pool accounting (pin; fixed in Step 4);
  - unlimited (-1) pool;
  - ALTER-while-running: old tx restores stale percent via `SetNewLimit` (pin; fixed in Step 4);
  - ExternalMemory-only allocation never touches the named pool (pin; fixed in Step 4);
  - rollback when Resource Broker rejects after pool reservation (`Y_DEFER` at `kqp_rm_service.cpp:330-346`);
  - concurrent acquire/release on one named pool;
  - pool spilling cookie flips at `SpillingPercent` threshold.

1b. `yql/essentials/minikql/ut/aligned_page_pool_ut.cpp` — the `IncreaseMemoryLimitCallback` contract is untested today:

  - callback raises limit → allocation proceeds;
  - callback denies (limit unchanged) → `TMemoryLimitExceededException`;
  - yellow-zone suppression while callback set and max-limit flag unset (`aligned_page_pool.cpp:672`);
  - `OffloadAlloc` (Arrow path) obeys the same limit/callback.

1c. `ydb/core/kqp/ut/query/kqp_limits_ut.cpp` / `ydb/core/kqp/workload_service/ut` — named-pool end-to-end:

  - pool with `TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE` small → heavy query in the pool fails `OVERLOADED` while a query in another pool succeeds (the pool analog of `ComputeActorMemoryAllocationFailure`, which covers the node-wide limit only);
  - ALTER RESOURCE POOL memory percent while a query runs;
  - serverless/shared database pool behavior (pin current).

Verification: `ya make ydb/core/kqp/rm_service -rA`, `ya make yql/essentials/minikql/ut -rA`, `ya make ydb/core/kqp/ut/query -rA -F '*KqpLimits*'`, `ya make ydb/core/kqp/workload_service/ut -rA`.

### Step 2 — Per-pool observability (rollout stage "observe-only")

  - Export per-(database, pool) sensors from `MemoryNamedPools`: Limit, Allocated, Peak, DeniedRequests, DeniedBytes, SpillingFlag; add a "WouldBeDeniedBytes" counter for enforcement changes planned later.
  - Pool table on the RM mon page (`kqp_rm_service.cpp:875-922`).
  - Counter groups must survive the erase-on-zero of `MemoryNamedPools` (`kqp_rm_service.cpp:400-404`). Implement the cheap variant (sensors stick to the map entry): Step 4 supersedes this by removing erase-on-zero entirely.

### Step 3 — Dead-code cleanup in the deny path

  - Remove the commented `yexception` in `dq_compute_memory_quota.h:145-146`; replace with a short comment: denial is enforced by `TAlignedPagePool::TryIncreaseLimit` throwing `TMemoryLimitExceededException`.
  - No behavior change; Step 1b/1c tests prove it.

### Step 4 — Fix named-pool correctness (feature-flagged where visible)

  - **Config ownership:** RM subscribes to pool updates like the CPU scheduler does (`TEvSubscribeOnPoolChanges` → `TEvUpdatePoolInfo`) and becomes the single writer of pool limits. `TTxState` drops `MemoryPoolPercent`; tx-path `SetNewLimit` removed. Pool entries live as long as the subscription, not until usage==0.
  - **Zero-percent semantics** per D2, behind feature flag (behavior change: today zero disables enforcement).
  - **ExternalMemory charged to the pool** per D6, behind the same flag: node service passes pool identity with initial reservations (`kqp_query_control_plane.cpp:323-329`); remove the `Memory==0` early-return bypass ordering at `kqp_rm_service.cpp:258-264`.
  - **D6 hot path is lock-free, one atomic RMW per op** (bench-driven design, see report §3.2/§3.4): pool account handle resolved once at tx registration and cached in `TTxState`; alloc/free = `pool->ExternalUsed.fetch_add/sub` (relaxed). Query level uses the existing `TxExternalDataQueryMemory` atomic; database/node aggregates are derived at publish cadence (`FireResourcesPublishing`), not per op — the node-wide atomic leaves the hot path. If D6 later needs denial (not just attribution): optimistic `fetch_add` + limit compare + rollback (ClickHouse pattern, R7), with documented bounded overshoot ≤ concurrent in-flight requests. Keeps OLTP queries (whose only RM interaction is this path) out of the RM lock convoy entirely. **Measured (prototype, 2026-08-09)**: external path 58 ns vs 54 ns baseline (locked MVP was 215 ns); OLTP cycle 259 ns vs 206 ns baseline (MVP 368 ns) — the +53 ns is the once-per-query handle resolution. Prototype: `bench_patches/d6_lockfree.patch` on branch `bench/memory-baseline` (paste: <https://paste.yandex-team.ru/3fea8b2d-c10e-451b-b0ad-c9d2585ce131/text>).
  - **Three separate feature flags**, not one: (a) zero-percent rejection (D2, user-visible semantics change), (b) ExternalMemory pool charging (D6, new denial source for over-limit pools), (c) config ownership (internal, lowest risk). Different blast radii must be independently revertable.
  - Pool entries are refcounted handles designed to be adopted by Step 5's `TMemoryAccountTree` without re-initialization — no second lifetime migration.
  - Flip the pinned tests from Step 1a.

### Step 5 — Extract the memory arbiter library

`ydb/core/kqp/rm_service/memory_arbiter/` + ut:

  - `TMemoryAccountTree`: node → database → pool → query accounts; atomic multi-level `TryAcquire`/`Release` with full rollback; single lock per request is acceptable (≥1 MiB batching, finding 1.3.5).
  - Refcounted account handles with a `Closed` state (accepts releases, denies acquisitions — allocations outlive queries).
  - Limit updates at any level; reduction below usage only denies growth.
  - Snapshot API for counters/mon.
  - `TKqpResourceManager` delegates: `TotalMemoryResource` → node account, `MemoryNamedPools` → pool accounts. RB interaction unchanged.
  - Unit tests: hierarchy enforcement, rollback, thread stress, dynamic limits, close-with-outstanding-bytes, saturation.
  - Prior-art additions (R1/R2/R6/R12): accounts carry `{guarantee, limit}` per D10, not just a limit; snapshot includes per-query `ReclaimableBytes` (0 until Step 7 consumers report it) and a `NonReclaimable` flag; a `RankVictims()` helper orders queries by (pool priority, used/guarantee, size) for the node-pressure path; deny/kill events recorded in an audit ring buffer exposed on the RM mon page.

  - Zone thresholds per account (green/yellow/red, §1.7): yellow generalizes the spilling cookie and is checked at acquire time; red raises the async reclaim event reserved by D12. Thresholds are PUSHed with the limit by the config owner.
  - Add `MEMORY_GUARANTEE_PERCENT_PER_NODE` to `TPoolSettings`, proto and DDL (D10 assignment) — data model only at this step; enforcement is Step 11's trigger.
  - Scaling gate (DoD, from report §3.4): re-run `rm_alloc_free_named_pool_contended{4,16,32}` on the arbiter binary; Δp50 ≤ +5% vs trunk at each concurrency level.

### Step 6 — Per-query limits

  - Lift validation at `resource_pool_settings.cpp:74-76` behind feature flag (D9); propagate through `kqp_planner.cpp` → `kqp_node_service.cpp` alongside the pool percent.
  - Query account at tx registration; limit = pool-effective limit × percent (D3); all allocations check the query level.
  - Denial follows the escalation chain (R3, TiDB/Trino pattern): (1) set the spilling hint and retry; (2) `TryShrinkMemory` (release free pages, lower limit) and retry; (3) behind D11 — register as a memory waiter in the arbiter, bounded async wait, woken by `FreeQuota` from completing queries; (4) fail with the D4 status and a message naming pool/limit **and the top-N pool consumers at denial time** (R9). Terminal failure stays a typed `TMemoryLimitExceededException`, never `yexception`.
  - Spill thresholds are ratios of the *effective* (most restrictive) limit in the chain — query grant vs pool limit vs node budget — recomputed when grants change (R8, ClickHouse PR \#71406 pattern), not absolute bytes.
  - Query-level stats into query statistics protobuf (peak, denied, spilled, limit, pool) — not dynamic counters (cardinality).
  - When D11 is off, the chain is explicitly (1)→(2)→(4) — the wait stage is skipped, not silently degraded; the D5 budget still bounds the whole episode.
  - Storm control (§13.3): per-query escalation state on `TTxState`/query account remembers that spill/shrink already failed in this episode — subsequent denials jump straight to their stage instead of re-walking the chain per 1 MiB request.
  - **DoD carries a "partial coverage" flag**: until Steps 7–9 land, limits cover MKQL/Arrow/channel allocations only; write-path memory is untracked and per-pool limits MUST NOT be advertised as node OOM protection.
  - Tests: per-query limit under/over pool; many queries in one pool; multi-node query enforcing locally on each node — including that a local fragment denial propagates cancellation to sibling fragments on other nodes via the existing KQP abort path.

### Step 6a — Memory-based admission gate (KR2, pulled forward from Step 11)

Without this, no new query is ever stopped for memory reasons before Step 11 — `IsAdmissionRequired()` ignores memory settings entirely. Flag-gated, observe-only counters first.

  - Workload service admission: for pools with a memory limit, check `pool.allocated + min_grant ≤ pool.limit` at admission; over → queue in the pool's existing FIFO (never fail immediately, never kill running queries).
  - `min_grant` fallback until ticket #16 (percentile feedback): conservative static estimate, default 10% of pool limit, config-overridable. Admission check is a fast-path atomic read, no actor round-trip.
  - Tests: pool at limit → new query queues; running query completes → queued query admitted; queue bounded by `QUEUE_SIZE`; observe-only mode counts would-be-queued without queueing.

### Step 7 — Complete cooperative coverage (one flag-gated PR each)

  - Channel buffers: route `TChannelQuotaManager` through the query account.
  - Stream-lookup / iterator read buffers: replace ad-hoc limits with account acquisitions.
  - Write-actor in-flight buffers.
  - Audit and eliminate `UseDefaultArrowAllocator()` in KQP execution paths.
  - Category dimension (Compute/Channel/Read/Write/ExternalSource) per acquisition.
  - Managed-vs-total gauge: pool managed bytes vs `GetGlobalPagePoolSize()` vs tcmalloc sampled bytes (residual measurement).

### Step 8 — Actor memory context

Introduce `NActors::NMemory` (interfaces per the design draft):

  - Optional `TMemoryContextPtr` member on `IActor` (no dynamic\_cast); guard installation in `executor_thread.cpp` next to the activity-tag switch (`executor_thread.cpp:272-277`), same only-on-change optimization.
  - `TMemoryBoundActorBootstrapped`; KQP compute actors bind their query account.
  - Benchmark gate before merge: event throughput no-context / empty-context / active-account, \<2% loss target.

### Step 9 — Event context for shared actors

  - Optional context on local `IEventHandle` (benchmark first; compact node-local id fallback if pointer regresses dispatch).
  - Propagation/detach APIs per the draft; reconstruction at Interconnect boundaries from existing KQP message fields.
  - First consumers: DataShard read and ColumnShard scan transient buffers.

### Step 10 — Sampled residual attribution (parallel from Step 5)

  - Composite (activity, pool) tcmalloc tag, interned per pool, bounded by `MaxTag=2048`; set from actor/event context when present.
  - Extend `mon_alloc/tcmalloc.cpp` per-tag export with the pool dimension.
  - Drives Step 7/9 prioritization and detects coverage regressions.

### Step 11 — Weighted memory admission (prioritization)

Yes to "prioritize memory like CPU" — but as *entitlement over new growth* (deny/spill/cancel borrowers), never instant reclamation.

  - Per D7/D10: pool entitlements from weights over the node budget, expressed as the guarantee/limit pair; borrowing allowed between guarantee and limit; entitlement \< usage → deny growth, spill pressure, cancel per victim ranking (R2), never instant reclamation.
  - Admission gating on memory (R4, Impala/Trino semantics): workload service admission checks `pool.reserved + initial_grant ≤ pool.limit`, where `reserved` counts admitted-but-unused grants (reservation ≠ consumption); over the soft limit → new queries queue in the pool's existing FIFO; running queries are never killed by admission.
  - Initial grant starts static (D3-derived); follow-up ticket adds percentile feedback from per-query-shape peak statistics (R5, SQL Server adaptive grants).
  - Periodic recalculation in the arbiter (cadence like `UpdateFairShare`), not in the CPU snapshot code.
  - **Floor-protection trigger (makes D10's guarantee real)**: when node usage \> Σ guarantees, new allocations from pools with `used \> guarantee` are denied/queued while pools with `used \< guarantee` continue to be granted. Without this rule the guarantee is a data model with no effect.
  - `initial_grant` before ticket #16: inherits Step 6a's static `min_grant`; document that grant=0 makes the check post-fact-only (observe phase).
  - Tests (DoD): reserved tracking inc/dec on admit/complete; queue-then-unblock on completion; floor protection — constrained pool below guarantee keeps allocating while borrower above guarantee is denied; admission check \< 2 µs per statement (bench gate).
  - If tree-sharing is preferred instead: first implement `TEvRemovePool`/`TEvRemoveDatabase` in the scheduler service (finding 1.3.4).

### Step 12 — Exact allocator-level tracking (separate track)

Unchanged from the draft: prototype-only, behind build/runtime flag, own performance gates. Decision input comes from Step 10 residual measurements.

### Step 13 — Cluster-wide pool/query limits over gossip (after per-node is stable)

Per §1.7 sequencing: only after Steps 1–6 (per-node) are enforced in
production. Mechanism (zverevgeny's design):

- Extend the existing `TKqpResourceInfoExchanger` snapshots with per-pool
  usage (bounded cardinality: pools only, never per-query).
- Workload service / arbiter consume the gossip aggregate to enforce
  cluster-level pool budgets and per-query cluster caps: admission and
  entitlement decisions only — never synchronous cluster coordination on the
  allocation hot path.
- Documented accuracy contract (D13): enforcement error is bounded by the
  gossip staleness window × aggregate allocation rate; short queries can
  complete inside one window and are intentionally governed only by
  node-local limits. Long-running queries converge to the cluster cap.
- Tests: two-node runtime (the existing exchanger tests' pattern), long
  query capped by cluster budget while node-local budget has headroom;
  short-query burst under-enforced but bounded; exchanger outage degrades to
  node-local enforcement (fail-open, counters flag staleness).

## 4\. Prototypes and Benchmarks (before/alongside implementation)

### 4.0a Measured (final, v3): tails, OLTP cycle, lock scalability (2026-08-09)

Worktree rebased to trunk `bb228c7f4c0`; interleaved A/B, **20 rounds × 100 iterations** (2000 samples per cell), medians across rounds; 57-core host, load ≈ 4 (mostly the benches themselves). Full methodology and per-decision analysis: `workload_manager_memory_bench_report.md`.

| Bench                                                        | base p50 | mvp p50 | Δp50     | Δp95   | Δp99           |
| ------------------------------------------------------------ | -------- | ------- | -------- | ------ | -------------- |
| no pool (control)                                            | 1849 ns  | 1883 ns | \+1.9%   | \+0.4% | −0.6%          |
| named pool                                                   | 2023 ns  | 2146 ns | \+6.1%   | \+4.4% | \+1.6%         |
| ExternalMemory-only                                          | 54 ns    | 213 ns  | \+294%   | \+268% | \+269%         |
| **OLTP query cycle** (tx + EU+external alloc/free + destroy) | 207 ns   | 366 ns  | **+77%** | \+73%  | \+71% (389 ns) |
| contended ×4                                                 | 27.8 µs  | 27.8 µs | −0.2%    | \+0.1% | \+1.5%         |
| contended ×16                                                | 139 µs   | 142 µs  | \+2.1%   | \+2.6% | \+2.8%         |
| contended ×32                                                | 287 µs   | 285 µs  | −0.8%    | −0.6%  | −0.7%          |

OLTP verdict: the MVP roughly doubles the *RM-side lifecycle cost of an OLTP query*, but the absolute number is +160 ns per query (p99 389 ns) — against the agreed E2E budgets (1–3 ms pin-point reads/writes, \~10 ms distributed transactions) that is 0.01–0.04%. Per-query RM cost is not an OLTP risk, and the D6 lock-free design (Step 4) removes even this delta plus the lock-convoy coupling.

Scalability finding (pre-existing, NOT caused by the MVP — deltas within the ±2% noise floor at every thread count): the current lock+ResourceBroker-instant path does not scale. Per-op latency grows linearly with threads (2 µs → 27.8 µs ×4 → 139 µs ×16 → 287 µs ×32), i.e. aggregate throughput *collapses* from \~500 kops/s single-threaded to \~110 kops/s at 32 threads (lock convoy + cache-line ping-pong). Consequences for the plan:

  - Step 5 arbiter: keep the single lock only together with ≥1 MiB batching (finding 1.3.5); add a microbench gate to Step 5's DoD: no regression of this curve.
  - If RM call frequency ever rises (smaller chunks, more callers — e.g. Step 7 coverage expansion), per-pool lock sharding or relaxed per-pool atomics must land first; D6's external charging is the first candidate to move to lock-free per-pool atomics.
  - The 32-thread numbers bound the worst case for OLTP bursts: even at full lock saturation an OLTP query's single RM cycle costs \~0.3 ms — visible, but only under a synthetic 32-thread allocation storm.

### 4.0 Measured: MVP-of-defaults vs current trunk (2026-08-07)

Worktree `bench/memory-baseline` (upstream main `a4562f46df0`, not committed): `KqpRmBench` suite in `ydb/core/kqp/rm_service/` + a 146-line MVP patch of the decision defaults (4-level node→database→pool→query accounting under the existing lock, per-query limit check, D6 ExternalMemory charged to pool+db, no erase-on-zero). Interleaved A/B, 5 rounds × 60 iterations × 2000 ops, quiet machine (23:00 MSK, load ≈ 3), control path drift \< 1%.

| RM path (alloc+free pair)         | trunk p50 | MVP p50 | delta                 |
| --------------------------------- | --------- | ------- | --------------------- |
| no pool (control, unchanged code) | 1902 ns   | 1917 ns | \+0.8% (noise floor)  |
| named pool                        | 2025 ns   | 2131 ns | **+5.2%** (\~+105 ns) |
| ExternalMemory-only               | 54 ns     | 217 ns  | **+303%** (\~+163 ns) |
| named pool, 4-thread contention   | 25.4 µs   | 25.4 µs | −0.1%                 |

MKQL page-pool churn (unchanged by MVP): \~120 ns/op; limit-callback adds \~5%; after freeing half the blocks the pool retains all pages (retained/used ≈ 2×) until shrink/`ReleaseFreePages` — the existing 30 MiB shrink hysteresis is the fragmentation control, not the arbiter.

Conclusions for the defaults:

  - **D3/D10 (query + database accounts): defensible.** +5% on a path invoked once per ≥1 MiB (min 30 MiB) growth chunk is invisible end-to-end.
  - **D6 (ExternalMemory to pool): defensible, with an absolute-cost caveat.** 4× relative, but +163 ns per reservation at task-start/channel-step frequency; if it ever shows up in profiles, per-pool relaxed atomics outside the lock are the escape hatch.
  - **Single-lock arbiter (Step 5): defended.** 4-thread contended cost (\~25 µs/op) is dominated by the existing lock+ResourceBroker-instant path; MVP accounting adds nothing measurable. Note the pre-existing \~13× contended-vs-single-thread ratio — batching (finding 1.3.5) is what keeps this acceptable, so do not lower quota chunk sizes without re-benching.

No production code changes; results recorded in the ticket.

1.  Arbiter contention microbench (scratch): 4-level tree, threads acquiring/releasing 1 MiB chunks — validates the single-lock choice.
2.  Actor dispatch benchmark baseline (existing benches in `ydb/library/actors/`) before Step 8; re-run with prototype context pointer.
3.  tcmalloc tag cardinality/overhead with a few hundred pool tags.
4.  Behavior bench: two pools with `TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE`, TPC-C + concurrent OLAP scan; record "before" for Steps 4/6.
5.  Coverage measurement: heavy query; RM-managed bytes vs `GetGlobalPagePoolSize()` vs RSS — quantifies the residual for Steps 7/9/12.
6.  Write-saturation bench (prior-art survey §13): sustained bulk upsert at ≥300 MB/s/node with limits enabled vs disabled — assert \<1% ingest throughput cost on the fast path, measure how much write-path memory is visible to pool accounts (coverage gap), and record pressure-path episode rates under saturation (deny/waiter/victim storm control).

## 5\. Follow-ups / Suggested Ticket Breakdown

One parent ticket + children, each ≈ one landable PR chain:

1.  Step 0 decisions D1–D12 (discussion, no code).
2.  Missing tests: RM named pools + page-pool callback + KQP pool limits (Step 1).
3.  Per-pool memory counters + mon page (Step 2).
4.  Deny-path dead-code cleanup (Step 3).
5.  Named-pool correctness fixes, feature-flagged (Step 4).
6.  Memory arbiter library + RM delegation (Step 5).
7.  Per-query limits end-to-end (Step 6).
8.  Coverage: channels / stream lookup / write actor / arrow (Step 7, 4 PRs).
9.  Actor context + benchmarks (Step 8).
10. Event context + shared actors (Step 9).
11. tcmalloc pool-dimension sampling (Step 10).
12. Weighted memory admission (Step 11, needs D7).
13. Exact allocator tracking prototype (Step 12; adopt ClickHouse-style TLS batched attribution — per-actor delta counter flushed at a \~4 MB configurable threshold — as the answer to counter contention, R7).

Prior-art follow-ups (from `workload_manager_memory_prior_art.md`, can trail the main chain):

14. Wait-for-quota memory waiters behind D11 (R3 stage 3), incl. deadlock escape hatch: waiter timeout + node-pressure victim ranking.
15. Node-pressure victim selection + kill/deny audit history on the mon page (R2); pre-OOM pressure alarm dumping top consumers to logs at \~70% of the node budget (R9).
16. Initial-grant percentile feedback from per-query-shape peak memory statistics (R5), after Step 2 counters and Step 7 coverage exist.
17. Arbiter reclaim phases (R6, Velox-style): consumers report `ReclaimableBytes`, arbiter strips free quota then commands spills before any abort — the interface hooks are reserved by D12 in Step 5.
18. Research track (SafeLoad-style, section 1.6): rule-based memory admission gate in the workload service at the `DatabaseLoadCpuThreshold` insertion point — start with an interpretable threshold rule over plan features + per-pool denial/OOM history; requires Step 2 counters and Step 6 per-query stats exposed as queryable system views; validate against SafeBench.
19. Cluster-wide pool/query limits over gossip (Step 13, D13): extend `TKqpResourceInfoExchanger` snapshots with per-pool usage; enforce cluster budgets in workload service admission/entitlement only.
20. Coordination with the hor911 zone-model track (§1.7): consume the RB→RM traffic-light zone flags when available; depend on the Memory Controller >100% distribution fix for overcommit configurations.

Docs follow-up: update pool-settings / `RESOURCE_WEIGHT` documentation when Steps 6 and 11 change user-visible semantics.

## 6\. Success Criteria and Risks

Success criteria as in the design draft, plus:

  - Step 4's flag flipped on changes nothing for pools without memory settings.
  - Step 3 changes nothing at all (proved by Step 1 tests).

Risk register deltas vs. the draft:

  - **Enforcement-tightening legacy:** pools that today silently exceed their percent (via ExternalMemory bypass / zero-percent) may start failing when Step 4's flag is enabled. Mitigation: Step 2 "WouldBeDeniedBytes" observe-only counter before any flag flip.
  - **Pool entry lifetime:** erase-on-zero (`kqp_rm_service.cpp:400-404`) conflicts with subscription-owned config — subscription becomes the owner; watch for pools deleted while queries run.
  - **Scheduler tree reuse temptation:** tree lacks remove support (`Y_ABORT` at `kqp_compute_scheduler_service.cpp:100,136`); do not couple memory account lifetime to it before that is fixed.
  - **Wrong-typed exceptions:** any new denial path must throw `TMemoryLimitExceededException` (or subclass), never generic `yexception`, or compute-actor status mapping breaks (see 1.2).
  - **Ingest-scale blind spot (survey §13):** until Steps 7–9 land, high-rate write memory (interconnect buffers, write-actor queues, columnshard insert/ compaction) is invisible to pool accounts — per-pool limits can look green while the node OOMs at \~300 MB/s ingest. Do not advertise admission gating as node OOM protection before write-path coverage; the node-level safety net (R2) must key off the Memory Controller's whole-process view, not the sum of pool accounts.
  - **Fast-path and pressure-path scale invariants (survey §13):** every accounted path must acquire in ≥chunk-sized units with actor-local batching (arbiter traffic stays \~10²/s even at 300 MB/s); any wait must be a mailbox suspension, never a blocked thread; polling-based nets need headroom ≥ `ingest_rate × (poll interval + cancel latency)`; deny/victim/waiter episodes need storm control (batched in-order wakeups, one kill per episode, per-query memory of failed escalation stages).
