# Workload Manager Memory Governance — Node-Wide Rework

Status: design proposal, 2026-08-14. Extends
`workload_manager_memory_limits_plan.md` (D1–D13, Steps 1–13) from KQP query
memory to **all memory categories of the node**, governed by the Workload
Manager. Companion: Memory Controller reference
(`memory_controller_config`, docs v26.1).

## 0. Program structure — three workstreams

| Part | Scope | Spec | Steps | Ships independently |
| --- | --- | --- | --- | --- |
| **A — KQP query limits** | database→pool→query accounting, per-query limits, admission, spilling escalation, cluster gossip | `workload_manager_memory_limits_plan.md` (D1–D13) | 1–13 | Yes — the current committed plan; no dependency on B/C |
| **B — Node memory manager** | WM as policy source over MC categories; category account tier; demand-driven rebalancing between KQP and shards | this doc, §§1–4, 6–7 (D14–D15) | 14–15 | After A's Step 5 (account tree) and Step 6 (escalation) |
| **C — Category expansion** | splitting the untracked activities into categories and handling each: BackupRestore, CDC, replication, index build, … | this doc, §5 (D16) | 16+ (one category per step/PR) | Each category after B's Step 14 (observe) — enforcement per category after Step 15 |

Part A is deliberately unblocked: everything measured and planned so far
(Steps 1–13) proceeds regardless of B/C decisions. Part B changes no
enforcement until its own flags flip. Part C is a repeatable per-category
recipe, not one big change.

## 1. Current model (Memory Controller, as documented) — Part B context

Per-node limits, static YAML config:

- **Hard limit** — process budget (cgroup-derived by default).
- **Soft limit** — danger threshold; above it the shared cache shrinks
  toward zero.
- **Target utilization** — the optimum the flexible caches size toward.
- **Cache components** (min/max percent, recomputed every second,
  proportional sharing): Shared cache, MemTable.
- **Activity components** (fixed individual limit + collective
  `activities_limit_percent` cap; over the cap → deny, near limits →
  spilling): Query Processor (`query_execution_limit_percent`),
  Compaction.
- Invariant: `shared_cache_min + mem_table_min + activities_limit <
  soft_limit`.
- **Uncovered**: backup/restore, CDC, replication and other activities
  have no individual limits today; auxiliary processes are untracked.

Mechanically, consumers already register with the Memory Controller
(`memory_controller_iface.h`: cache/memtable/CS consumer kinds with
`SetLimit`/consumption reporting), and the query-execution budget reaches
KQP as a Resource Broker queue limit. What is missing is not mechanism but
**policy and feedback**: the split between categories is a static config
chosen at deploy time, blind to the workload.

## 2. The gap this rework closes

1. **No policy plane.** Whether a node is OLTP-hot (wants shared cache),
   ingest-hot (wants MemTable + compaction), or analytics-hot (wants query
   execution) is a *workload* property — exactly what the Workload Manager
   knows and the Memory Controller cannot.
2. **No demand feedback.** The 1-second cache loop divides by configured
   proportions, not by observed demand/pressure; activity components
   cannot borrow from idle caches or from each other.
3. **No category for maintenance flows.** Backup/restore et al. compete
   inside the untracked remainder and can push the node into the soft
   limit unattributed.
4. **hor911 alignment**: the zone-model presentation marks Memory
   Controller "N/A" in the PUSH/PULL table and lists "fix MC to
   distribute >100% in any configuration" as a TODO — this rework is the
   umbrella for both.

## 3. Part B — node memory manager: detailed design

### 3.1 Components and responsibilities

| Component | Role | Change |
| --- | --- | --- |
| **Workload Manager (policy engine)** | computes desired category budgets from workload signals and operator policy; publishes a versioned advisory | NEW: category policy module in the workload service |
| **Memory Controller** | per-node mechanism: clamps advisory against hard/soft/target invariants, runs the per-cycle rebalance, PUSHes limits+zones to consumers | EXTENDED: advisory input, demand-driven sizing, zone thresholds |
| **Consumers** (SharedCache, MemTable, CS caches, Compaction, KQP RM, later BackupRestore…) | report usage/demand/reclaimable; obey pushed limits and zone signals | EXTENDED: report shape; KQP RM becomes a first-class consumer |
| **Resource Broker** | unchanged: admission queue for query-execution tasks; its `kqp_rm` queue limit becomes a *derived output* of the QueryExecution category | none beyond config source |

Non-goals (unchanged from the plan): replacing MC or RB; synchronous
cross-component waits; per-query cluster coordination.

### 3.2 Category account model

The Step 5 arbiter account tree gains a root tier. Category account fields:

```text
TCategoryAccount {
  Kind        // SharedCache | MemTable | Compaction | QueryExecution |
              // ColumnTables* (existing kinds) | BackupRestore | ...
  Guarantee   // floor: bytes the category can always reach
  Limit       // ceiling: max bytes it may try to use
  Used        // last reported consumption
  Demand      // last reported unconstrained want
  Reclaimable // last reported async-releasable bytes
  YellowAt, RedAt  // zone thresholds, derived per cycle
  Version     // advisory config version that produced the limits
}
```

Invariants (checked by MC each cycle, violations clamp + increment a
sensor):

1. `Σ Guarantee(cache_min + activities) < soft_limit` — the documented MC
   invariant, now enforced dynamically rather than at config parse.
2. `Guarantee ≤ Limit` per category; `Σ Limit` MAY exceed soft limit
   (overcommit is the point — hor911 "MC >100%").
3. Category set v1 = existing `EMemoryConsumerKind` + QueryExecution +
   Compaction; adding a kind is a Part C event, not a schema change.

### 3.3 Advisory config: schema, delivery, clamping

Proto sketch (new message, `memory_controller_config.proto` companion):

```text
message TMemoryAdvisory {
  uint64 Version = 1;
  message TCategory {
    EMemoryConsumerKind Kind = 1;
    optional uint32 GuaranteePercent = 2;  // of hard limit
    optional uint32 LimitPercent = 3;
    optional uint32 YellowPercent = 4;     // of category limit, default 80
    optional uint32 RedPercent = 5;        // default 95
  }
  repeated TCategory Categories = 2;
}
```

- **Source**: workload service publishes to each node's MC via the same
  subscription pattern as pool configs (Step 4); node-local cache of the
  last advisory survives WM restarts.
- **Clamping**: MC applies `min(advisory.Limit, invariant-derived max)`
  and `max(advisory.Guarantee, component floor)`; every clamp increments
  `Advisory/ClampedCount` with the category label. The advisory can never
  make MC violate its own hard/soft/target math.
- **Bootstrap and fallback**: until the first advisory arrives — and
  whenever the advisory is older than `AdvisoryTtlSeconds` (default 300) —
  MC uses the static YAML exactly as today. Rolling upgrade: old nodes
  ignore the advisory message entirely.
- **Config surface (open question §7.1 narrowed)**: v1 is cluster-level
  (one advisory for all nodes, distributed via console config); per-node
  differentiation only through percent-of-hard-limit semantics.

### 3.4 Consumer protocol

Extends `memory_controller_iface.h` (all changes additive):

```text
// today: IMemoryConsumer { SetConsumption(ui64) }
struct TConsumerReport {
  ui64 Used;
  ui64 Demand;       // would-use-if-unconstrained
  ui64 Reclaimable;  // can free asynchronously on request
};
// IMemoryConsumer gains SetReport(TConsumerReport); SetConsumption
// remains as the degraded form (Demand=Used, Reclaimable=0).

// today: TEvConsumerLimit { LimitBytes }
// extended: + GuaranteeBytes, YellowBytes, RedBytes  (zone PUSH)
```

Per-kind demand definitions (the load-bearing part):

| Consumer | Demand | Reclaimable |
| --- | --- | --- |
| SharedCache | current size + miss-driven want (bytes missed over the last cycle, EWMA) | clean pages |
| MemTable | current size + ingest backlog (bytes buffered upstream of flush) | flushable memtables (via existing `TEvMemTableCompact`) |
| Compaction | queued compactions × per-task estimate (RB queue already tracks this) | 0 (tasks finish, not shrink) |
| QueryExecution (KQP RM) | Used + queued admission grants + denied-bytes EWMA (Step 2 counter `WouldBeDeniedBytes`) | Σ per-query `ReclaimableBytes` from the D12 snapshots (spillable state) |
| BackupRestore (Part C) | in-flight buffers + scheduled-task estimate | pausable scan buffers |

Report cadence: piggybacked on the existing MC wakeup cycle
(`memory_controller.cpp` `HandleWakeup`, interval-driven); consumers
report at most once per cycle — no new hot-path cost.

### 3.5 Rebalancing algorithm (per MC cycle)

```text
1  clamp advisory → {guarantee, limit} per category      (§3.3)
2  reserve guarantees: every category may always grow to its guarantee
3  flexible budget F = target_utilization − Σ min(used, guarantee)
4  distribute F to cache categories proportional to DEMAND
   (not static percents), bounded by [guarantee, limit]
5  activities borrowing: idle cache headroom (allocated-but-unused
   below cache limits) is lendable to activity categories above their
   guarantee, up to their limit — recorded as Borrowed(cat)
6  derive zones: YellowAt = YellowPercent × limit;
   RedAt = RedPercent × limit; if process total > soft_limit,
   force red on every category above its guarantee (borrowers first,
   largest Borrowed first — the D10 victim rule at category level)
7  PUSH TEvConsumerLimit{limit, guarantee, yellow, red} where changed
```

Properties: monotone under pressure (a category above guarantee can only
shrink), work-conserving (idle memory is lent), never blocks an
allocation path (denial is against current pushed limit), converges in
O(cycles) because step 4 uses EWMA demand.

### 3.6 Zone semantics per consumer

| Consumer | Yellow (stop optional growth) | Red (release asynchronously) |
| --- | --- | --- |
| SharedCache | stop admission of new pages beyond current size | evict toward guarantee (existing shrink path) |
| MemTable | prefer flush over growth | force flush (existing `TEvMemTableCompact`) |
| QueryExecution | RM sets the spilling cookie node-wide (existing `SpillingPercent` machinery) | forced spill per victim ranking (Step 6 escalation, initiated by RM not by the query) |
| Compaction | defer new low-priority tasks | cancel/requeue deferrable tasks |
| BackupRestore | shrink batch/in-flight windows | pause scans until green |

### 3.7 Failure model

- **Silent consumer** (no report ≥ N cycles): keeps last pushed limit;
  its limit decays linearly toward its guarantee over `DecayCycles`
  (default 30) — a wedged consumer cannot hold borrowed memory forever.
- **WM outage / stale advisory**: TTL expiry → static YAML fallback
  (§3.3); in-flight category accounts keep working, only the *policy*
  reverts.
- **MC restart**: consumers re-register (existing flow); first cycle
  rebuilds state from reports; until then consumers keep last limits.
- **Version skew**: advisory carries Version; MC ignores versions ≤ last
  applied; consumers ignore zone fields they do not understand
  (additive proto).

### 3.8 KQP integration specifics

- KQP RM registers as a QueryExecution consumer
  (`TEvConsumerRegister{QueryExecution}` — new kind) and receives
  `TEvConsumerLimit` directly; the RB `kqp_rm` queue limit is then set
  *by MC from the same value* (single writer), removing today's implicit
  contract where the RB config override is the only budget channel
  (finding 1.3.2 / the "128 GB decorative default" incident from the
  TPC runs).
- The pool/query tree (Part A) hangs unchanged under the QueryExecution
  category account: pool percents apply to the category limit — i.e.
  today's semantics, but the base can now move per cycle. Pool limits
  recompute on category-limit change via the existing `SetActualLimits`
  path (bounded staleness: one cycle).
- Yellow/red for QueryExecution reuse the Part A cookie and forced-spill
  machinery; no second signaling path.

### 3.9 Compatibility and rollout

Flags (all default off, independent):
`enable_memory_advisory` (WM→MC policy), `enable_demand_rebalancing`
(step 4-5 of the algorithm; off = today's proportional cache math),
`enable_category_zones` (zone PUSH; off = only limits move).
Rollout: observe-only first — MC computes-but-does-not-apply the new
limits and exports both (`Category/{kind}/AppliedLimit` vs
`.../AdvisedLimit`); flip per flag after side-by-side soak. Static YAML
remains authoritative whenever every flag is off; a node can be reverted
by config alone.

### 3.10 Observability

Per category: `Limit, Guarantee, Used, Demand, Reclaimable, Borrowed,
Zone (0/1/2), ClampedCount, DecayActive`; node: `AdvisoryVersion,
AdvisoryAgeSeconds, FallbackActive`. All bounded cardinality (categories
are an enum). Existing MC `Stats/*` sensors unchanged.

### 3.11 Testing

- Algorithm unit tests (pure function over report vectors): guarantee
  satisfaction, demand proportionality, borrowing, red-zone ordering,
  decay, clamp invariants — table-driven.
- `memory_controller_ut.cpp` extension: consumer registration with
  reports, limit/zone PUSH, fallback on TTL expiry, version skew.
- Ingest stress (prior-art §13 scenario): MemTable demand spike must
  shrink SharedCache to guarantee within K cycles without soft-limit
  breach.
- HTAP scenario on the tier harness (`bench_tiers.sh htap` + Part B
  flags): OLAP burst borrows cache headroom, returns it after; TPC-C
  tpmC unaffected within same-binary spread.

### 3.12 Risks

- **Oscillation** (cache↔activity thrash): EWMA demand + hysteresis
  (release requires M consecutive cycles of lower demand); gate: cache
  size variance sensor.
- **Reclaimable overstatement**: red-zone release is best-effort; the
  soft-limit backstop (force-red cascade, then existing shared-cache
  shrink-to-zero) still holds regardless of reports.
- **Config complexity**: three flags + advisory — mitigated by
  observe-only sensors and YAML-only fallback.

## 4. Part C — category expansion: detailed design

### 4.1 Framework: what "becoming a category" means

A category is: an `EMemoryConsumerKind` entry + a consumer actor
registered with MC + buffers routed through a category account +
policy/dashboard presence. The recipe from §0 formalized:

1. **Register**: the activity's controller registers
   `TEvConsumerRegister{kind}` at startup; receives the
   `IMemoryConsumer` handle (existing flow, zero new infrastructure).
2. **Route**: buffers acquire/release through a `TCategoryCharge`
   RAII token bound to the account (same `TryAcquire/Release` +
   ownership-token pattern as the design draft's `TMemoryCharge`);
   batched at buffer granularity, never per-object.
3. **Observe**: category appears in §3.10 sensors; enforcement OFF —
   denials counted (`WouldBeDeniedBytes`), not enforced.
4. **Enforce**: per-category flag; yellow/red actions wired per §3.6.

Acceptance per category (DoD): observe-mode data for ≥1 production-like
run showing demand shape; unit tests for acquire/rollback; a stress test
demonstrating yellow and red actions; dashboards.

### 4.2 BackupRestore (first)

**Where the memory is** (why this category is first):

- Export path: DataShard scan buffers (`backup_unit.cpp` /
  `export_*` units read table data into memory batches) and S3
  upload buffers (multipart part buffers per in-flight upload).
- Import path: S3 download buffers + parsed-row batches queued for
  `UploadRows` into shards.
- All of it currently lives in the untracked process remainder — a wide
  restore can push a node over the soft limit with no attribution.

**Design:**

- Consumer registration in the export/import controller actor per node
  (one consumer, kind `BackupRestore`; per-task accounting inside).
- Charges: one `TCategoryCharge` per scan batch and per S3 part buffer;
  granularity = existing batch sizes (MiB-scale, no hot-path concern).
- Yellow: halve the in-flight window (concurrent parts / batches per
  task) — throughput degrades, memory flattens.
- Red: pause issuing new scan batches; in-flight uploads complete and
  release; resume on green. Pause/resume uses the tasks' existing
  retry/backoff machinery — no new persistence.
- Defaults: `Guarantee = 2%`, `Limit = 10%` of hard limit (validated in
  observe mode); backup SLA impact is acceptable by definition — it is
  the deferrable workload.
- Tests: restore of a large table under a tight category limit completes
  (slower, no OOM, no failure); concurrent backup + TPC-C: tpmC
  unaffected; kill -9 during red-pause leaves no leaked charges
  (accounts are node-local and die with the process).

### 4.3 CDC initial scan

Same shape as export: scan batches + sink buffers. Consumer at the CDC
stream controller; charges per batch; yellow shrinks batch window, red
pauses the scan (the CDC protocol already tolerates pauses — offsets).
Distinct category (not folded into BackupRestore) because its pressure
correlates with user-facing replication lag — operators need to see it
separately.

### 4.4 Async replication

Initial-sync phase ≈ CDC initial scan (same recipe); steady-state
apply buffers are small but bursty on reconnect — guarantee sized for
steady state, limit for reconnect bursts.

### 4.5 Index build

Build scan + intermediate sort/batch buffers; controller = the build
operation actor. Red action = spill the sort run (index build already
supports restarts/checkpoints, so pause is also acceptable as v1).

### 4.6 Rollout order and rationale

| # | Category | Why this order |
| --- | --- | --- |
| 1 | BackupRestore | biggest unattributed consumer; zero user-latency SLA |
| 2 | CDC initial scan | same recipe, adds the lag-visibility requirement |
| 3 | Async replication | reuses 2; reconnect-burst sizing is the only novelty |
| 4 | Index build | needs the spill action — depends on Part A Step 6 machinery being stable |

One PR per category; each independently flagged and revertable; a
category in observe mode indefinitely is a valid end state (attribution
alone already solves the "who ate the node" problem).

## 5. Decisions

| # | Decision | Default | Acceptance |
| --- | --- | --- | --- |
| D14 | WM is the memory policy source for MC category budgets: versioned advisory, MC-clamped, TTL fallback to static YAML | flag, observe-only first | side-by-side sensors ≥1 week; zero clamp violations at defaults |
| D15 | Demand-driven rebalancing per §3.5 + consumer report extension per §3.4; overcommit guarantee→limit is normal; red zone = async release | flag, off | ingest-stress and HTAP tests green; oscillation sensor below threshold |
| D16 | Every maintenance activity becomes a category per §4.1 recipe; no untracked activity above its guarantee under node pressure | staged, one PR per category | per-category DoD (§4.1) |

## 6. Steps

- **Step 14 — Category accounts + advisory (observe-only)**: §3.2 model
  in the arbiter, §3.3 plumbing behind `enable_memory_advisory`,
  §3.10 sensors. No enforcement change.
- **Step 15 — Demand rebalancing + zones**: §3.4 report extension,
  §3.5 algorithm behind `enable_demand_rebalancing`, §3.6 zone wiring
  behind `enable_category_zones`, §3.7 failure model, §3.11 tests.
- **Step 16+ — Part C categories**: §4.2→§4.5 in the §4.6 order.

Dependencies: Step 14 needs Part A Step 5 (account model); Step 15's
QueryExecution red action needs Step 6 (forced spill); Part C Step 16+
needs Step 14 (observe) per category, Step 15 for enforcement.

## 7. Open questions for the design review

1. Advisory scope v1: cluster-level only (per §3.3) — is per-database
   category policy (serverless tenants) required earlier than D13's
   cluster gossip work?
2. May QueryExecution borrow above `activities_limit` (true overcommit,
   §3.5 step 5) in v1, or activities borrow only from cache headroom?
3. Red-zone ordering among activity categories: fixed (§4.6 reverse
   order) or WM-weighted?
4. Does Compaction demand come from RB queue depth (cheap) or a
   dedicated estimator (accurate)? v1 proposal: RB queue depth.
5. `EMemoryConsumerKind` is compiled-in; do we need a dynamic category
   registry for Part C velocity, or is one enum entry per PR acceptable?
   v1 proposal: enum is fine.
