# Prior art: in-process memory resource management in mono-binary DBMSs

Companion to `workload_manager_memory_limits_plan.md`. Surveys how other databases and query engines govern memory *inside a single process*, and distills which mechanisms are worth adopting for YDB's workload-manager memory limits. Sources are public docs/source of the respective projects; per-system references are listed inline.

YDB baseline for comparison (see the plan, §1.4): compute actors obtain quota through `IMemoryQuotaManager` → `TKqpResourceManager` (node-wide `TotalMemoryResource` + per-(database, pool) named counters), the mkql `TAlignedPagePool` enforces the granted limit via the increase-limit callback, and denial surfaces as a typed `TMemoryLimitExceededException` that the compute actor maps to `OVERLOADED`. Admission is per-resource-pool in workload service; memory plays no role in admission today.

-----

## 1\. ClickHouse

The closest analog to YDB: a C++ mono-binary with its own allocator wrapper, full-process memory attribution, and (recently) SQL-defined workloads.

### 1.1 MemoryTracker hierarchy with thread-local batching

Parent chain: thread-local counter → query `MemoryTracker` → user tracker → server `total_memory_tracker`. Every `new`/`delete` goes through the ClickHouse allocator which calls `CurrentMemoryTracker::alloc()/free()`; deltas accumulate in a TLS counter and flush upward only when they exceed `max_untracked_memory` (4 MB default), so the cost per allocation is a TLS add, and atomics/limit checks fire once per \~4 MB. Threads attach to a query's `ThreadGroupStatus` (which owns the query tracker), so multi-threaded queries aggregate naturally. Since 2025 (PR \#84082) even third-party C `malloc` is captured via linker `--wrap`. Caches (mark cache, dictionaries) charge the server tracker explicitly; jemalloc metadata and tcache remain untracked (RSS drift, §1.4).

### 1.2 Enforcement

Limit check happens inside the tracker on flush: `current + delta > limit` → throw `DB::Exception` code 241 `MEMORY_LIMIT_EXCEEDED` with the triggering level in the message ("for query" / "for user" / "total"). Limits: `max_memory_usage` (10 GB default per query), `max_memory_usage_for_user`, `max_server_memory_usage(_to_ram_ratio)` (0.9 of RAM).

### 1.3 Memory overcommit: guarantees + victim selection instead of first-toucher failure

Since 22.5. With overcommit on, per-query limits become soft; the server limit is the hard ceiling. On hitting it, the allocating query does not fail immediately:

1.  It *waits* on the `OvercommitTracker` (up to `memory_usage_overcommit_max_wait_microseconds`).
2.  The tracker picks a victim: the query with the highest ratio `allocated / memory_overcommit_ratio_denominator` — i.e. the one most over its *guaranteed* share. Denominator 0 = never a victim (protected queries).
3.  Victim is cancelled; if enough memory frees in time the waiter resumes, else the waiter throws 241.

Key semantics: the denominator is a per-query/per-user **memory guarantee**, and pressure is resolved against whoever is most over guarantee — not against whoever happened to allocate last.

### 1.4 jemalloc/RSS reconciliation: MemoryWorker + cgroup observer

`MemoryWorker` background loop refreshes jemalloc epoch stats, computes dirty-page bytes, and purges arenas when dirty \> 10 GiB or \> 20% of tracked memory; PR \#94902 split it into a fast RSS-update thread and a slow purge thread so purge latency can't delay limit corrections. `CgroupsMemoryUsageObserver` reads cgroup v2 `memory.current` (fixed to exclude reclaimable page cache after issue \#64652) and corrects `total_memory_tracker`. Observable gap = `MemoryResident` − `MemoryTracking` async metrics; `SYSTEM JEMALLOC PURGE` closes it manually.

### 1.5 Workloads: memory as a schedulable resource (experimental)

`CREATE RESOURCE memory (MEMORY RESERVATION)` + `CREATE WORKLOAD ... SETTINGS max_memory = '10Gi'` + query-level `reserve_memory`: a query's reservation sits in *pending* state until the workload has headroom (admission queueing), then *running*; under pressure running reservations can be evicted (query killed) ordered by workload `precedence`, max-min fair by weight within a tier. Coexists with (does not replace) MemoryTracker hard limits; doesn't yet cover merges/mutations. CPU and IO scheduling in the same workload tree are further along (token buckets, preemptive CPU slots).

### 1.6 Spilling tied to the effective limit

`max_bytes_ratio_before_external_group_by/sort`: the threshold is computed as `ratio × min(query, user, server limit)` by walking the tracker chain (PR \#71406), so spill points auto-scale with deployment instead of being absolute bytes.

Key refs: `src/Common/{MemoryTracker,OvercommitTracker,MemoryWorker,CgroupsMemoryUsageObserver}.*`, <https://clickhouse.com/docs/operations/settings/memory-overcommit>, <https://clickhouse.com/docs/operations/workload-scheduling>, <https://github.com/ClickHouse/ClickHouse/issues/84976> (workloads umbrella), <https://github.com/ClickHouse/ClickHouse/issues/41887> (reclaimable memory proposal).

-----

## 2\. CockroachDB

### 2.1 BytesMonitor / BoundAccount tree (`pkg/util/mon`)

The canonical Go implementation of a hierarchical accounting tree:

  - **Hierarchy**: root monitor (node-wide, bounded by `--max-sql-memory`, default 25% of RAM) → session monitor (per connection) → query/flow monitor → operator monitor. A child monitor's budget is itself a `BoundAccount` opened against the parent, so charges propagate upward.
  - **Amortized reservation**: `BoundAccount.Grow()` drains a local pre-reserved buffer first and goes upstream only in `poolAllocationSize` chunks (10 KB default), releasing excess when it exceeds 10 chunks. This keeps lock contention on the shared root low.
  - **Denial is an error value**, not a callback: `"memory budget exceeded"` propagates to the operator; the vectorized engine converts it into an internal panic caught at operator boundaries.
  - **Spill vs fail split**: the per-operator soft budget `sql.distsql.temp_storage.workmem` (64 MiB default) triggers disk spill (external sort, grace hash join, etc. via Pebble temp storage); the *node-global* `--max-sql-memory` budget has no spill fallback — exhaustion is a hard query error to protect the node.
  - **Off-heap caches**: the Pebble block cache (`--cache`, 25% of RAM) is deliberately allocated off the Go heap via CGo malloc so the GC never sees it; it is budgeted at startup, not tracked per-allocation.

### 2.2 Admission control explicitly excludes memory

CockroachDB's admission control (slot/token queues ordered by tenant → priority → tx start time) governs CPU and store I/O only. Their tech notes call out why memory is omitted: memory is non-preemptible, and slowing down some layers can make things *worse* by prolonging how long already-allocated memory is held. Memory backpressure lives entirely in the BytesMonitor tree. This is a useful caution for YDB: memory-aware throttling of *running* work is a different problem from memory-aware *admission*, and the former can backfire.

Key refs: `pkg/util/mon/bytes_usage.go`, `pkg/sql/colmem/allocator.go`, `docs/tech-notes/admission_control.md`, <https://www.cockroachlabs.com/blog/disk-spilling-vectorized/>, <https://www.cockroachlabs.com/blog/memory-usage-cockroachdb/>.

-----

## 3\. TiDB

### 3.1 MemTracker tree + the OOMAction escalation chain

Per-session/per-executor `MemTracker` tree (`util/memory`); `Consume()` walks ancestors and, when any node exceeds its limit, fires that node's **chain of escalating actions** (a linked list, each with a fallback pointer):

1.  `rateLimitAction` — reduce parallelism first: stop one concurrent table-scan thread (`tidb_enable_rate_limit_action`).
2.  Spill actions — operator-specific: `SpillDiskAction` (hash/merge join), `SortAndSpillDiskAction`, `AggSpillDiskAction` (switches HashAgg to spill mode).
3.  Terminal action — `PanicOnExceedAction` (CANCEL, default: query cancelled with per-operator memory breakdown in the error) or `LogOnExceedAction` (LOG: continue, just log). Configured by `tidb_mem_oom_action`.

Per-query soft budget: `tidb_mem_quota_query` (1 GiB default).

### 3.2 Global memory controller: kill-top-consumer

Since v6.5: `tidb_server_memory_limit` (default 80% of RAM). A background goroutine polls Go `HeapInuse`; on breach it repeatedly kills the session with the largest tracked consumption until under the limit (DDL and some query classes exempt). Kill events are auditable via `INFORMATION_SCHEMA.MEMORY_USAGE_OPS_HISTORY` (last 50 events: timestamp, SQL digest, memory).

### 3.3 Pressure alarm with diagnostic dumps

`tidb_memory_usage_alarm_ratio` (default 0.7): on crossing the threshold (rate-limited to once per minute and only if usage grew \>10%), TiDB dumps goroutine stacks, a heap profile, and the top-10 queries by memory/duration to rolling files — diagnostics captured *before* the OOM, not after.

### 3.4 Allocator/runtime reconciliation

The gctuner adjusts Go `GOMEMLIMIT`/GC cadence against the server limit; the controller reads `HeapInuse`, accepting that stacks and runtime metadata are untracked. Same story as CockroachDB: neither Go system fully reconciles tracked bytes with RSS; they bound what they can and leave headroom.

Key refs: <https://docs.pingcap.com/tidb/stable/configure-memory-usage/>, <https://pingcap.github.io/tidb-dev-guide/understand-tidb/memory-management-mechanism.html>, `util/memory/tracker.go`, `util/gctuner/memory_limit_tuner.go`.

-----

## 4\. Meta Velox (used by Prestissimo / Presto C++)

The most architecturally interesting design for YDB: memory is **arbitrated between running queries**, with reclaim-by-spilling preferred over deny-and-fail.

### 4.1 MemoryPool tree with logical capacity separate from physical allocation

Per-query pool tree mirroring the execution tree (root query pool → task → node → operator; only leaf pools touch the physical allocator). Each root query pool has a *current* logical `capacity_` that the arbitrator can grow/shrink at runtime, and a hard `maxCapacity_` fixed at query start. Physical allocation (`MemoryAllocator`) and logical budget (`MemoryArbitrator`) are distinct layers — capacity can move between queries without touching pages.

### 4.2 SharedArbitrator: the growCapacity algorithm

When a reservation would exceed the query's capacity, the pool asks the arbitrator to grow it. The algorithm, serialized under a global lock:

1.  **Self-check**: if the requestor is already at its own `maxCapacity`, try self-reclaim (spill its own operators); if still over — fail locally (no victims).
2.  **Fast path**: strip *free* capacity (reserved-but-unused) from other queries via `shrink()` — pure bookkeeping, no I/O.
3.  **Slow path**: sort candidates by `reclaimableBytes()` (estimated spillable state) and command them to reclaim: query reclaimer → task reclaimer (pauses the task's drivers) → `Operator::reclaim()` which spills hash tables / sort runs / window state to disk. The *arbitrator* initiates spilling, not the operator.
4.  **Last resort**: `handleOOM()` aborts the query with the **largest current capacity** and retries. Requestor threads "enter arbitration" in a suspended state so their own task can be paused for reclaim without deadlock.

Operators mark `nonReclaimableSection_` around critical phases (e.g. mid hash-table build); the arbitrator skips them.

### 4.3 Notable properties

  - Victim selection is capacity-based, not priority-based (priority tiers were discussed upstream but not implemented) — a gap YDB's resource pools could fill.
  - Spilling is externally commanded, so the policy lives in one place; operators only implement the mechanism (`reclaim()` + `reclaimableBytes()`).

Key refs: <https://facebookincubator.github.io/velox/develop/memory.html>, <https://facebookincubator.github.io/velox/develop/spilling.html>, `velox/common/memory/{MemoryPool,MemoryArbitrator,SharedArbitrator}.h`.

-----

## 5\. DuckDB

### 5.1 Unified buffer manager

One global `memory_limit` (default 80% of RAM) governs *all* memory — storage pages and operator intermediates alike — in 256 KB blocks with pin/unpin semantics and LRU eviction to `temp_directory`. Operators do not write spill code: they *unpin* blocks, and the buffer manager transparently evicts under pressure. Spill policy is fully decoupled from operator logic.

### 5.2 TemporaryMemoryManager: negotiation among concurrent operators

Solves "two hash joins each sized to 60% of the limit → 120%". Memory-intensive operators register a `TemporaryMemoryState` (RAII), declare their remaining need (`SetRemainingSize`), and receive a fair reservation ≈ `min(60% of limit / active_count, need)`, with `SetMinimumReservation` as a starvation floor. Reservations are re-negotiated as operators join/complete; an operator whose reservation is below its data size switches itself to a partitioned/out-of-core algorithm. Implemented for hash join and radix-partitioned aggregation.

Key refs: <https://duckdb.org/2024/07/09/memory-management>, <https://github.com/duckdb/duckdb/pull/10147>, <https://duckdb.org/2024/03/29/external-aggregation>.

-----

## 6\. Apache DataFusion

`MemoryPool` is a trait; operators call `try_grow()` before allocating and must spill-and-retry on denial. Implementations:

  - **GreedyMemoryPool** — single atomic counter, FCFS, no fairness.
  - **FairSpillPool** — splits consumers into unspillable (FCFS) and spillable; each spillable reservation is capped at `(pool_size − unspillable_memory) / num_spillable_reservations`. The pool never commands spilling — it only denies growth; the operator self-spills. Known failure mode (issue \#17334): a non-spillable operator can starve when spillable ones consume reactively — evidence that *deny-only* fairness without arbitration is fragile.
  - **TrackConsumersPool** — decorator that annotates OOM errors with the top consumers by name and bytes (cheap, high-value diagnostics).

Key refs: <https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/>, <https://github.com/apache/datafusion/issues/17334>.

-----

## 7\. Trino / Presto

### 7.1 Deny → block, and the death of the reserved pool

Presto's historic two-pool design (general + reserved, where the single biggest query migrated to the reserved pool on all workers under pressure) was **removed** in Trino (\#6677): the cluster-wide migration caused unpredictable memory jumps and was a band-aid. What remains:

  - Per-worker user/total limits (`query.max-memory-per-node`, `query.max-total-memory-per-node`) and cluster-wide aggregates (`query.max-memory`), plus explicit `memory.heap-headroom-per-node` for untracked JVM allocations.
  - **Blocked state**: a task that cannot get memory *blocks* (stops consuming CPU, waits for memory to free) instead of failing immediately.
  - **OOM killer** as the deadlock escape hatch: `query.low-memory-killer.policy` = `total-reservation` (kill the cluster-wide biggest query) or `total-reservation-on-blocked-nodes` (kill retryable tasks on the nodes that are actually out of memory). One kill per trigger, then re-evaluate.
  - Memory is split into user / system / **revocable** (spillable) categories; revocable memory is counted against total but not user limits.

### 7.2 Resource groups: admission gating on memory, no kills

`softMemoryLimit` per resource group: when the group's aggregate consumption exceeds it, **new queries queue**; running queries are untouched. Combined with `hardConcurrencyLimit`, `maxQueued`, and weighted/fair scheduling policies. This cleanly separates "admission control by memory budget" from "enforcement by killing".

Key refs: <https://trino.io/docs/current/admin/resource-groups.html>, <https://trino.io/docs/current/admin/properties-memory-management.html>, <https://github.com/trinodb/trino/issues/6677>, <https://prestodb.io/blog/2019/08/19/memory-tracking/>.

-----

## 8\. Apache Impala

MemTracker hierarchy (process → query → fragment → operator) with **two quantities per node: consumption and reservation**. The distinctive part is admission control:

1.  The planner estimates per-host memory from table stats; admission clamps it into the pool's `[min-query-mem-limit, max-query-mem-limit]`.
2.  Admission gates on the pool's aggregate `max-memory` **including reservations of admitted but not-yet-hungry queries** — preventing the thundering herd where many queries admitted against current consumption all allocate at once.
3.  Every blocking operator must obtain its **minimum buffer reservation** (enough to run, even if spilling heavily) up front; if no executor can satisfy it, the query is *rejected at admission* rather than admitted and killed mid-flight.
4.  Otherwise queries **queue** (`max-queued-queries`, `queue-timeout-ms`).

Key refs: <https://impala.apache.org/docs/build/html/topics/impala_admission.html>, <https://impala.apache.org/docs/build/html/topics/impala_buffer_pool_limit.html>.

-----

## 9\. SQL Server

### 9.1 Memory grants and the resource semaphore

The optimizer computes a **minimum** and **desired** memory grant per query. Grants are acquired from a per-pool *resource semaphore*: if memory is unavailable the query **waits** (`RESOURCE_SEMAPHORE`) up to a cost-based timeout, may be degraded to its minimum grant (spill-heavy execution), and only errors (8645/8657) as a last resort. Resource Governor pools carry `MIN/MAX_MEMORY_PERCENT`; workload groups cap a single query's share (`REQUEST_MAX_MEMORY_GRANT_PERCENT`, default 25% of the pool) — and the engine first *reduces DOP* to shrink the requirement before refusing.

### 9.2 Adaptive memory grant feedback

Post-execution feedback corrects the estimate for future executions of the same plan: spills → grant increased; grant \>2× used → decreased; since SQL Server 2022 the feedback is percentile-based and **persisted in Query Store**, surviving plan-cache eviction. Feedback never exceeds the workload-group cap.

Key refs: <https://learn.microsoft.com/en-us/sql/relational-databases/performance/intelligent-query-processing-memory-grant-feedback>, <https://learn.microsoft.com/en-us/sql/t-sql/statements/create-workload-group-transact-sql>.

-----

## 10\. Greenplum / Cloudberry

Resource groups with a **fixed + shared** split of each group's memory: every transaction gets a guaranteed slice (`MEMORY_QUOTA / CONCURRENCY`); a shared portion (`MEMORY_SHARED_QUOTA`, default 80%) is burstable FCFS. `MEMORY_SPILL_RATIO` sets where operators start spilling within the budget. Accounting is a **Vmem tracker** intercepting every `palloc()` per process, bounded globally by `gp_vmem_protect_limit`. A **runaway detector** activates at `runaway_detector_activation_percent` (90% default) of the node ceiling and kills top consumers until below threshold (system group exempt). Admission queues transactions when concurrency slots or memory slices are unavailable.

Key refs: <https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum/7/greenplum-database/admin_guide-workload_mgmt_resgroups.html>.

-----

## 11\. Comparison matrix

| System          | Attribution                                       | Hierarchy                          | On per-consumer limit                                   | On node/pool pressure                                                                 | Prioritization                                                 | Admission on memory                                                                                      | Untracked-memory reconciliation                                     |
| --------------- | ------------------------------------------------- | ---------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| **ClickHouse**  | allocator hook + TLS batching (4 MB)              | thread→query→user→server           | throw (code 241) or overcommit-wait                     | victim = max(used/guaranteed), waiter blocks with timeout                             | overcommit denominator = guarantee; workload precedence (exp.) | experimental (`MEMORY RESERVATION` pending state)                                                        | MemoryWorker purge loop + cgroup observer correcting the tracker    |
| **CockroachDB** | explicit accounting (BoundAccount), 10 KB chunked | root→session→flow→operator         | error → operator spills (workmem) or query fails (root) | none (first-toucher fails at root)                                                    | none for memory; admission is CPU/IO only                      | **explicitly rejected** (memory non-preemptible)                                                         | off-heap cache carved out at startup; RSS monitored, not reconciled |
| **TiDB**        | explicit accounting (MemTracker.Consume)          | session→executor→component         | **action chain**: throttle scans → spill → cancel/log   | global controller kills top consumer until under limit; audit table                   | exemptions (DDL, CTE); otherwise size-based                    | none                                                                                                     | gctuner drives GOMEMLIMIT; polls HeapInuse, stacks untracked        |
| **Velox**       | pool tree reservations (leaf pools allocate)      | root query→task→node→operator      | grow request → arbitration                              | **arbitrator**: strip free quota → command spills by reclaimableBytes → abort largest | capacity-based victim (no priorities yet)                      | n/a (embedded)                                                                                           | allocator/arbitrator layers separated by design                     |
| **DuckDB**      | buffer manager owns all memory (pin/unpin)        | single global pool + TMM states    | reservation shrinks → operator switches to out-of-core  | transparent LRU eviction of unpinned blocks                                           | fair share `min(60%/N, need)` + minimum floors                 | n/a (embedded)                                                                                           | unified — everything is buffer-managed                              |
| **DataFusion**  | explicit try\_grow/shrink reservations            | pool→consumer→reservation          | Err → operator self-spills and retries                  | none (deny-only)                                                                      | fair cap `(pool−unspillable)/N_spillable`                      | n/a (embedded)                                                                                           | n/a                                                                 |
| **Trino**       | explicit local memory contexts                    | operator→task→query→worker→cluster | task **blocks** on memory, doesn't fail                 | coordinator OOM killer (total-reservation policies), one kill per trigger             | resource-group weights; user/system/revocable classes          | **yes**: group softMemoryLimit queues new queries                                                        | `heap-headroom-per-node` carve-out for untracked JVM                |
| **Impala**      | MemTracker consumption + **reservation**          | process→query→fragment→operator    | spill within reservation, else query fails              | admission prevents most; post-admission kill if over                                  | pool min/max clamps on query limits                            | **yes**: gate on pool max-memory incl. reservations; reject if min reservation unsatisfiable; else queue | process mem\_limit as ceiling                                       |
| **SQL Server**  | grants (semaphore accounting)                     | pool→workload group→query grant    | degrade to minimum grant, spill                         | **wait** (RESOURCE\_SEMAPHORE), DOP reduction, then error                             | Resource Governor MIN/MAX\_MEMORY\_PERCENT per pool            | **yes**: grant semaphore is the admission gate                                                           | buffer-pool-internal                                                |
| **Greenplum**   | palloc interception (Vmem tracker)                | segment→group→transaction          | spill at MEMORY\_SPILL\_RATIO, error at slice           | runaway detector kills top consumers at 90% of node ceiling (system group exempt)     | fixed slice + shared FCFS tier per group                       | **yes**: queue on concurrency/memory slots                                                               | gp\_vmem\_protect\_limit node ceiling                               |

Convergent findings across all ten systems:

1.  **Nobody punishes the first-toucher at node scope.** Every system that survived production contention resolves *global* pressure via victim selection (most-over-guarantee or largest), waiting, or arbitration — never by failing whichever query happened to allocate next. CockroachDB, the one system without this, documents "memory budget exceeded" storms as a known pain. YDB today is first-toucher at both pool and node scope.
2.  **Two-tier limits are universal**: a soft threshold that changes *behavior* (spill, throttle, shrink) and a hard threshold that changes *outcome* (cancel). The soft tier is per-operator or ratio-of-limit; the hard tier is per-query/pool/node.
3.  **Admission gating on memory works and killing mid-flight is the fallback, not the design.** Trino/Impala/SQL Server/Greenplum all gate admission on pool memory (reservations, not just consumption); systems without it (TiDB, ClickHouse pre-workloads) had to bolt on kill-top-consumer safety nets.
4.  **Arbitration (Velox) is the frontier**: logical capacity moves between running queries, reclaimed by externally-commanded spilling, with abort only as a last resort. It requires operators to report `reclaimableBytes()` and implement a reclaim entry point — a contract, not a rewrite.
5.  **Everyone reconciles tracked bytes with the allocator/OS view** with an explicit carve-out (headroom config) or a correction loop (MemoryWorker, cgroup observers, GOMEMLIMIT tuners).

-----

## 12\. Transferable ideas and recommendations for YDB

Ordered by leverage against the plan (`workload_manager_memory_limits_plan.md`); each maps to a plan step or proposes a new follow-up. All are doc-level proposals — no code was touched.

### R1. Guaranteed vs burstable memory per resource pool (ClickHouse denominator + Greenplum fixed/shared)

Give each resource pool a **memory guarantee** (derived from `RESOURCE_WEIGHT` or a new `MEMORY_GUARANTEE_PERCENT_PER_NODE`) alongside the existing limit. The invariant: sum of guarantees ≤ node budget ≤ sum of limits (overcommit allowed between guarantee and limit). Pressure between pools is then resolvable by a well-defined ranking: `used / guarantee`. → Extends plan Step 5 (arbiter data model) and Step 11 (weighted admission); the same guarantee/limit pair is what ClickHouse, Greenplum, and SQL Server (MIN/MAX\_MEMORY\_PERCENT) all converged on.

### R2. Victim selection instead of first-toucher denial at node scope (ClickHouse, TiDB, Trino, Greenplum)

Today, when `TotalMemoryResource` is exhausted, the query that asks next gets denied — regardless of who caused the pressure. Add a node-level arbiter pass: when a request cannot be granted, rank running queries by (pool priority, `used/guarantee`, size) and cancel — or first ask to shrink/spill (R3) — the top consumer, while the requester waits with a timeout (async, via actor mailbox — never blocking a thread). Keep an **audit ring buffer** of kill/deny events (TiDB's `MEMORY_USAGE_OPS_HISTORY`) surfaced on the RM mon page. → This is the single biggest behavioral improvement available; slots into plan Step 5. Feature-flagged, default off initially (same rollout discipline as the plan's D-decisions).

### R3. Escalating action chain on the quota-denial path (TiDB OOMAction; Velox reclaim)

Replace the binary "granted / not granted" result of `IMemoryQuotaManager::AllocateQuota` with an ordered escalation the compute side already half-implements:

1.  **spill** — `IsReasonableToUseSpilling` already exists; make denial *first* set the spilling hint and retry once;
2.  **shrink** — ask the actor to `TryShrinkMemory` (release free pages, lower limit);
3.  **wait** — bounded async wait for quota (Trino blocked state / SQL Server semaphore), registered with the RM as a memory waiter, woken on `FreeQuota` from completing queries;
4.  **fail** — typed `TMemoryLimitExceededException` (never `yexception` — plan §6 risk). Steps 1–2 are cheap and local; step 3 is the feature-flagged new mechanism; step 4 is today's behavior and remains the terminal state. → Refines plan Steps 5–6; the chain structure means each stage can ship as its own ticket.

### R4. Admission gating on pool memory reservations, not consumption (Impala, Trino softMemoryLimit)

Workload service admission (already queue-based for concurrency) should also check: `pool.reserved_memory + query_initial_grant ≤ pool.memory_limit`, where `reserved` counts admitted-but-not-yet-allocated grants (Impala's reservation-vs-consumption distinction — prevents the thundering herd of simultaneously admitted queries). Over the soft limit → new queries queue in the pool's existing FIFO; running queries untouched (Trino semantics). → Strengthens plan Step 11 with precise semantics, and reuses the workload service queue instead of inventing a new one.

### R5. Initial-grant estimation with execution feedback (SQL Server adaptive grants)

The admission check in R4 needs a per-query grant size. Start with the static `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE`-derived value, then add a feedback loop: record peak tracked memory per query shape (KQP already computes a query hash for the compiled-query cache) and use a high percentile of recent executions as the next admission grant, clamped to pool min/max (Impala clamps). SQL Server's history shows percentile-based beats last-execution-based (oscillation). → New follow-up ticket (plan §5); depends on Step 2 counters + Step 7 coverage.

### R6. Arbiter evolution target: two-phase reclaim before abort (Velox SharedArbitrator)

Long-term shape for the plan's memory arbiter: on a grow request that exceeds the pool/node budget — (a) strip *free* quota (granted-but-unused; `TryShrinkMemory` makes this real) from other queries, bookkeeping only; (b) command spills on candidates ranked by reclaimable bytes (compute actors already know spillable state size; add `reclaimableBytes` to the RM snapshot); (c) only then abort by R2 ranking. Requires two contracts actors must implement: report reclaimable bytes; handle an async "reclaim N bytes" event. Velox's `nonReclaimableSection` maps to phases where an actor cannot spill (mid hash-table build) — a flag in the snapshot. → This is the "new way" the survey was looking for: it converts memory from a grant-once-then-fail resource into a continuously arbitrated one, matching how the CPU scheduler already treats CPU. Design it into Step 5's interfaces now (even if phases (b)/(c) ship later) so the arbiter API doesn't need breaking changes.

### R7. Batched attribution for exact allocator tracking (ClickHouse max\_untracked\_memory)

Plan Step 12 (exact allocator tracking) should adopt the TLS-batch pattern: per-actor/thread delta counter flushed to the pool counter at a threshold (start at 4 MB like ClickHouse; make it a config knob since it trades precision-of-enforcement for contention). This is also the answer to the plan's concern about counter contention on `MemoryNamedPools` (absl map behind a mutex): batch at the source instead of sharding the sink.

### R8. Ratio-based spill thresholds tied to the effective limit (ClickHouse PR \#71406)

Where spill thresholds exist (`SpillingPercent` cookies), define them as a ratio of the *most restrictive* limit in the chain (query grant, pool limit, node budget), recomputed when grants change — not absolute bytes. Auto-scales across instance sizes. → Small amendment to Step 6.

### R9. Diagnostics parity (DataFusion TrackConsumersPool, TiDB alarm dumps)

Cheap, high-value, shippable early:

  - Annotate every `TMemoryLimitExceededException` (and the OVERLOADED issue built from it) with the top-N consumers of the relevant pool (name, bytes) at denial time.
  - A pressure alarm at \~70% of the node budget that dumps top queries by tracked memory to logs (rate-limited), *before* the OOM.
  - The R2 audit history on the mon page. → Fold into plan Step 2 (counters) and Step 9 (mon/observability).

### Explicitly *not* recommended

  - **Presto-style reserved pool** — removed upstream as a design mistake (\#6677); don't reinvent it as "emergency pool per node".
  - **Memory-aware throttling of running work as admission substitute** — CockroachDB's admission notes explain why slowing running work can *increase* memory held; keep admission (R4) and running-query arbitration (R6) as separate mechanisms.
  - **Uncommenting the historical throw in `dq_compute_memory_quota.h`** — unchanged from the plan: the denial path must remain typed; R3 formalizes what happens *before* the typed failure.

-----

## 13\. Scale stress-test: which approaches survive sustained \~300 MB/s ingest per node

The prior art must be read with a caveat: **in most surveyed systems the memory manager never sits on the ingest hot path at this rate** — either the write path bypasses the tracker, or the system bottlenecks on something else (planner, coordinator, disk, GC) before the memory manager's own scalability limits surface. Below: the event-rate math for YDB's chain at 300 MB/s/node, then which borrowed mechanisms break first and why the donor system never noticed.

### 13.1 Event-rate budget for YDB's chain at 300 MB/s

Assume \~1 KB rows → \~300 K rows/s/node; KQP data flows in \~48 KB–1 MB chunks; page pool grants ≥1 MiB (`MinMemAllocSize` = 30 MiB first grant, 1 MiB alignment after).

| Path                                                           | Op granularity        | Ops/s at 300 MB/s                             | Verdict                                                                                                                                                                                              |
| -------------------------------------------------------------- | --------------------- | --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Page-pool extra-memory requests (`RequestExtraMemory`)         | ≥1 MiB (30 MiB first) | ≤300/s worst case, \~10/s typical             | Trivial. The 1 MiB batching already built into `TDqMemoryQuota` is the single most load-bearing design property; arbiter lock contention is a non-issue at this rate.                                |
| Arbiter `TryAcquire` (Step 5), same granularity                | ≥1 MiB                | ≤300/s                                        | Single mutex fine; no sharding needed.                                                                                                                                                               |
| Admission checks (Step 11 / R4)                                | per statement         | \= statement rate: 3–50 K/s for small upserts | **Risky if it adds an actor round-trip per statement.** Needs a lock-free fast path: check an atomic `pool.reserved` snapshot locally, take the queue path only when within \~10% of the soft limit. |
| Channel / write-actor buffer acquisitions (Step 7)             | per chunk, 48 KB–1 MB | 300–6 000/s                                   | OK **only if acquisitions stay chunk-sized**. If Step 7 naively accounts per-row or per-cell, this becomes 300 K–millions/s → must batch at the actor (R7 applies to Step 7, not just Step 12).      |
| Allocator-level tracking (Step 12) via malloc hooks, unbatched | per allocation        | millions/s                                    | Fails. Only viable with ClickHouse-style TLS batching (4 MB batch → \~75 flushes/s/thread at 300 MB/s).                                                                                              |
| Pressure-path events (victim ranking, waiter wakeups)          | per pressure episode  | must be rate-limited by design                | O(queries·log) ranking is fine *per episode*; the risk is episode frequency under sustained saturation (see 13.3).                                                                                   |

Overshoot math for any *reactive* mechanism: `overshoot ≈ ingest_rate × reaction_latency`. At 300 MB/s: allocation-time denial (today's typed exception) ≈ 0 overshoot; an event-driven victim kill with \~100 ms cancel latency ≈ 30 MB; a 1 s polling loop (TiDB-style controller, RSS workers, RB limit reconfiguration) ≈ **300 MB per interval** — headroom between the soft threshold and the node hard limit must be sized to at least `rate × (poll + cancel latency)`, or the safety net fires after the kernel OOM killer does.

### 13.2 Why each donor system never hit these limits — and what breaks if copied naively

  - **ClickHouse** genuinely sustains hundreds of MB/s of ingest through its tracker — but only because of two properties: 4 MB TLS batching (without it, the parent-chain atomics at millions of ops/s would collapse on cache-line contention), and an insert path made of few large columnar buffers rather than many small allocations. What does *not* transfer: the overcommit wait **blocks an OS thread** per stalled query. CH tolerates this because inserts rarely enter overcommit (insert-block memory is bounded); with thousands of concurrent small upserts stalled, a thread-blocking wait would exhaust the pool. In YDB the D11 wait must be a mailbox suspension — this is not an optimization but a survival requirement.
  - **TiDB**'s kill-top-consumer controller *polls* Go `HeapInuse` and cancellation lands asynchronously seconds later; its headroom (kill at 80% of RAM, alarm at 70%) is implicitly sized for TiDB's ingest rates, which are bottlenecked by the planner/RPC layer long before 300 MB/s/node of tracked churn. Copied naively into YDB, a polling controller at 300 MB/s eats its whole headroom in one interval. YDB's advantage: enforcement is *allocation-driven* (page-pool callback), so the primary limit has zero reaction latency — keep the polling controller strictly as the second-line net and size its threshold with the overshoot formula.
  - **Velox**'s SharedArbitrator serializes all arbitration under a global lock, and reclaim means seconds of spill I/O while the requester waits. This works because Prestissimo runs *few, long* analytic queries — arbitration is rare. Under 3–50 K small statements/s, any design where a routine grow request can enter global arbitration would serialize the node. Transfer rule: arbitration must be entered only above a size floor (e.g. requests beyond the pool guarantee) and rate-limited; small requests below guarantee follow the fast grant/deny path. This is why the plan keeps R6 phases (b)/(c) deferred behind D12 interfaces rather than on the day-one grant path.
  - **Trino/Impala/SQL Server** admission gates survive high QPS because the gate is an in-memory semaphore/counter check (SQL Server handles 10⁴+ grants/s), *not* a message hop. Their weakness at scale is different: coordinator/statestore aggregation lags by heartbeats (Impala explicitly documents over-admission races between daemons). YDB's per-node enforcement doesn't inherit that flaw — but the workload-service queue must not become a per-statement actor round-trip (fast path above).
  - **DuckDB**'s TemporaryMemoryManager renegotiates shares under a lock across *all* registered states on every join/leave — perfect for ≤dozens of big operators in an embedded process, quadratic-ish churn at thousands of concurrent registrations/s. Adopt the fair-share formula for the few large spillable consumers (per-pool), never as a per-query registration mechanism.
  - **Greenplum**'s per-`palloc` Vmem interception costs a check on *every* allocation; GP tolerates it because segment ingest is bottlenecked by dispatch/network. It is the cautionary tale for Step 12 without batching.
  - **CockroachDB** is the most instructive parallel for the *coverage* question: its ingest path (KV/Pebble memtables) bypasses the SQL BytesMonitor tree entirely — the SQL memory manager simply does not see bulk-write pressure, and node protection comes from separately-budgeted memtables/cache plus headroom. See 13.3.

### 13.3 The honest YDB-specific conclusions

1.  **The coverage gap is the real 300 MB/s risk, not manager throughput.** At high ingest rates most memory pressure lives *outside* KQP compute quotas until Steps 7–9 land: interconnect event buffers, write-actor in-flight queues, datashard/columnshard insertion and compaction buffers (Memory Controller consumers). Like CockroachDB's SQL monitor, the workload-manager limits will be accurate for compute and *blind to ingest* in the interim — per-pool limits can look green while the node OOMs from write-path memory. Consequence: (a) Step 2's managed-vs-total gauge must be watched specifically under write-heavy load, (b) admission gating (R4) must not be advertised as node OOM protection until Step 7 coverage of the write path exists, (c) the second-line node net (R2) must key off the Memory Controller's whole-process view (RSS/soft limit), not off the sum of pool accounts.
2.  **The chunked-grant design is what makes everything else affordable.** ≥1 MiB grant granularity caps arbiter traffic at \~300 ops/s even at 300 MB/s — decisions can stay behind one mutex, victim ranking can be exact, audit logging is free. Preserve this invariant in Step 7: every new accounted path must acquire in chunk-sized units with actor-local batching, or it becomes the new bottleneck.
3.  **Pressure paths need storm control, not just correctness.** Under sustained saturation (ingest indefinitely above budget), deny/victim/wait episodes recur continuously: waiter wakeups on every `FreeQuota` must wake in grant order and in batches (thundering herd), victim selection must be rate-limited (one kill per episode, re-evaluate — Trino semantics), and the escalation chain (R3) must remember per-query that spill/shrink already failed rather than re-walking the chain on every 1 MiB request.
4.  **The fast path must stay one atomic check.** At 300 MB/s the node's true limiter may be disk, compaction, or interconnect — memory governance must never be the thing that shows up in the flamegraph first. Benchmark gate (plan §4) should include a write-saturation scenario: bulk upsert at ≥300 MB/s with limits enabled vs disabled, asserting \<1% ingest throughput cost, alongside the existing TPC-C + OLAP-scan scenario.

## 14\. Recent research (arXiv)

Three papers reviewed against the plan (`workload_manager_memory_limits_plan.md`). ID note: "2502.0943" is truncated; it resolves to 2502.09431, the only cs.DB paper in the 2502.0943x range.

| Paper                                                                        | What it is                                                                                                                        | Relation to the plan                                                                                                   |
| ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| SafeLoad, arXiv:2601.01888 (PVLDB Vol 9 No 4, VLDB 2026; Alibaba AnalyticDB) | ML admission control: predict memory-overloading (MO) queries between optimizer and executor, reroute them to serverless clusters | Complementary layer *above* runtime enforcement; source of a concrete future admission gate (plan ticket \#18)         |
| Buffer management survey, arXiv:2512.22995                                   | 4-decade survey: LRU-K/2Q/LIRS/ARC → ML policies → disaggregated memory (RDMA/NVM, eBPF)                                          | Confirms cache/query-memory separation (design non-goal); ideas for Memory Controller budget balancing and spill tiers |
| NVM as primary storage, arXiv:2502.09431 (PostgreSQL)                        | Storage engine adapted to NVM + helper-thread prefetching, −45..54% query time                                                    | Peripheral; validates helper-thread attribution requirement and cheap-spill trend                                      |

**SafeLoad — what to follow.** Their pipeline: (1) an interpretable threshold rule ("\#joins \> 1 OR \#windows \> 0, AND scan output \> 1 MB, AND cluster had OOM yesterday") disposes of 97.8% of queries in 401 ns; (2) per-cluster XGBoost models (clusters with ≥100 MO samples; global model otherwise) over 163 plan+cluster features; (3) a FAISS index of past false negatives catches retried failures (cosine ≥ 0.9999); (4) an entropy-priced per-cluster quota on positive predictions — their single largest precision lever (0.55 → 0.81; +66% max vs best baseline; 8.09× less wasted CPU).

  - **Order of work validated.** SafeLoad is admission-only: a false negative still OOMs (recall 0.89 → \~11% of MO queries land anyway) and is merely recorded for next time; the safety net is rerouting to provider-paid serverless clusters. YDB has no such overflow tier and shares nodes across tenants, so runtime enforcement (plan Steps 4–7) must come first; prediction is an optimization on top, not a substitute.
  - **Classification over regression.** They show peak-memory *regression* is the wrong tool for admission (their regression baseline F1 ≤ 0.38; spikes are transient, available memory shifts between submit and execute). Tempers R5: percentile grant feedback is for *initial sizing*, not for admission verdicts.
  - **Rule-before-ML staging fits.** An equivalent cheap rule can run in the workload service with zero ML infrastructure at the same insertion point where `DatabaseLoadCpuThreshold` already gates (post-compile, pre-execution). Their key feature — "OOM events on this cluster yesterday" — maps to per-pool `DeniedRequests`/OOM counters: plan Step 2 counters and Step 6 per-query stats should be queryable (system view), not sensors-only, so they can serve as features and labels later.
  - **FP/FN asymmetry.** For them a false positive burns user quota; for YDB a false "will overload" would deny/queue a safe query — user-visible. Any future auto-deny must mirror their quota-throttled positives (bounded rate of prediction-driven denials).
  - **SafeBench** (150M production queries, github.com/SafeLoad-project/SafeBench) is usable as an external validation corpus for a YDB predictor prototype.

**Buffer-management survey — what to follow.** Cache memory is elastic and evictable; query working memory is occupancy — different control loops. This confirms keeping shared caches under Memory Controller (design non-goal) rather than folding them into the arbiter. The survey's feedback/utility-driven budget balancing is a plausible later evolution of Memory Controller's binary-search consumer split; it would reach the arbiter only through the existing RB queue-limit channel (plan finding 1.3.2), so no arbiter interface change is needed. Disaggregated-memory/NVM spill tiers are a long-horizon option for D5: the cheaper spilling gets, the more aggressive spill-first (R3) can be.

**NVM paper — what to follow.** Its helper threads prefetching on behalf of a query are precisely the cross-thread attribution case plan Step 9's event context and `TMemoryContextGuard` must support — keep that requirement first-class. Its results reinforce the same cheap-spill trend as above.
