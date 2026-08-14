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

## 3. Proposed architecture: WM = policy, MC = mechanism

```text
Workload Manager (policy)
    │ category budgets {guarantee, limit, zones} — advisory, versioned
    ▼
Memory Controller (mechanism, per node)
    │ PUSH limits + green/yellow/red thresholds
    ├── SharedCache consumer      (cache;    DataShard/ColumnShard pages)
    ├── MemTable consumer         (cache;    write path)
    ├── Compaction consumer       (activity; background)
    ├── QueryExecution consumer   (activity; → RB queue → KQP RM
    │                              → database→pool→query tree, Steps 1–13)
    ├── BackupRestore consumer    (activity; NEW category)
    └── ...                       (CDC, replication — later)
```

- The **category layer is the same account model** as the pool tree one
  level up: each category carries `{guarantee, limit}` (D10 semantics),
  green/yellow/red thresholds (§1.7 zone model), and the D12 snapshot
  contract (`used, demand, reclaimable`). No new concepts — the Step 5
  arbiter's account tree gains a root tier.
- **WM advises, MC enforces.** WM computes desired category budgets and
  pushes them to MC as a versioned advisory config (same subscription
  pattern as pool configs, Step 4). MC clamps them against its invariants
  (hard/soft/target) and remains the final authority — a broken WM cannot
  OOM the node.
- **Static YAML remains the fallback** and the bootstrap default; the WM
  advisory path is feature-flagged (default off) and reported
  side-by-side in observe-only mode first.

## 4. The rebalancing protocol (KQP ↔ shards ↔ MC)

Extends the existing consumer registration; one round per MC cycle (1 s):

1. **Report (PULL up)**: every consumer reports `{used, demand,
   reclaimable}` — demand = what it would use if unconstrained (cache
   miss pressure, ingest backlog, queued query grants), reclaimable =
   what it can free asynchronously (clean cache pages, spillable query
   state, flushable MemTable).
2. **Decide**: MC (with the WM advisory as the objective) computes new
   category limits: caches proportional to *demand* rather than static
   percents; activities may borrow idle cache headroom above cache
   guarantees; the D10 rule applies across categories — a category above
   its guarantee is first to shrink under pressure.
3. **PUSH down**: new limits + zone thresholds delivered to consumers.
   Yellow → consumer stops optional growth (KQP: spilling hint; shards:
   stop cache growth). Red → consumer must release asynchronously
   ("what it can, when it can" — hor911 red-zone semantics; KQP: forced
   spill per victim ranking; SharedCache: evict; MemTable: flush).
4. **Never synchronous**: no allocation path ever waits on a rebalance
   round; denial happens against the *current* limit (same rule as the
   pool tree).

The KQP↔shard "protocol" is therefore not a new peer-to-peer channel: it
is mediated by the MC cycle through demand/reclaimable reports — shards
and KQP never talk to each other directly, which keeps the failure model
simple (a silent consumer keeps its last limit and decays toward its
guarantee).

## 5. Part C — category expansion (per-category recipe)

Each untracked activity becomes a category by the same four-move recipe
(one PR per category):

1. Register an MC consumer for the activity (`{guarantee, limit}`,
   zones, D12 snapshot).
2. Route the activity's buffers through the category account
   (`TryAcquire/Release`, Step 5 API).
3. Add the category to WM policy + dashboards (observe-only first).
4. Flip enforcement per category once observe-mode data confirms the
   budget shape.

Order (by blast radius and current pain): **BackupRestore** first —
backup/restore memory currently lives untracked in the process remainder
and can push a node into the soft limit unattributed; then CDC initial
scan, async replication, index build — each bursty and deferrable,
exactly the shape the guarantee/limit + red-zone model handles.

## 6. Decisions added

| #   | Decision | Proposed default |
| --- | --- | --- |
| D14 | WM is the memory *policy* source for MC category budgets (advisory, versioned, clamped by MC invariants; static YAML = fallback and bootstrap) | Yes, feature flag, observe-only first |
| D15 | Demand-driven rebalancing protocol: consumers report `{used, demand, reclaimable}` per MC cycle; caches sized by demand; activities may borrow above-guarantee cache headroom; red zone = async release | Yes — extends existing consumer iface, no new channel |
| D16 | Every maintenance flow gets a category (BackupRestore first); no untracked activity may exceed its category guarantee under node pressure | Staged, one category per PR |

## 7. Steps added

- **Step 14 — Category accounts + WM advisory (observe-only).** Root tier
  in the arbiter account model; WM → MC advisory config plumbing behind a
  flag; side-by-side sensors: static-config limits vs WM-advised limits vs
  actual. No enforcement change.
- **Step 15 — Demand-driven rebalancing.** Extend consumer reports with
  demand/reclaimable; switch cache sizing to demand-proportional within
  min/max; activity borrowing above guarantees; red-zone async release
  wired to SharedCache eviction, MemTable flush, and KQP forced spill
  (reuses Step 6's escalation machinery).
- **Step 16+ (Part C) — one step per category**, starting with
  BackupRestore, following the §5 recipe. Independent, repeatable,
  individually revertable.

Dependencies: Steps 14–16 build on Step 5 (account tree), Step 6
(escalation), D10 (guarantee), D12 (reclaim contract), D13's gossip is
orthogonal (cluster dimension vs category dimension). hor911's "MC >100%"
TODO becomes a D15 property: overcommit between guarantee and limit is
the normal state, safe because red-zone release is enforced.

## 8. Open questions for the design review

1. Where does category policy live in config/DDL — a `workload_manager`
   section, per-database resource-pool-like objects, or cluster-level
   only? (Categories are node-scoped; databases share them.)
2. May QueryExecution borrow above `activities_limit` when caches are
   idle (true overcommit), or only within it? (hor911 TODO says yes;
   invariants must then be enforced dynamically, not statically.)
3. Priorities *between* categories under red pressure: fixed order
   (caches shrink first?) or WM-configurable weights?
4. Serverless/multi-tenant: are category guarantees per-node only, or do
   tenants get category sub-guarantees (ties into D13 cluster gossip)?
