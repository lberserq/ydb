# Workload Manager Memory Limits

Status: design draft

## Summary

Add per-node workload memory limits using the existing hierarchy of database,
resource pool, and query. Reuse the CPU scheduler's registration model and
pull-based consumers, but do not apply CPU throttling semantics directly to
memory.

Memory is outstanding state rather than a renewable rate. CPU can be delayed
and resumed, while allocated memory cannot be reclaimed immediately. Hard
memory limits can reject future growth. Priority can affect new grants and can
request cooperative spilling or cancellation, but it cannot provide immediate
reclamation without preemption.

Introduce a memory context that attributes quota-aware allocations to a
database, resource pool, query, and allocation category. A
`TMemoryBoundActorBootstrapped` binds dedicated actors to this context. Shared
actors use an event context. The actor executor installs the selected context
in TLS while the actor handles an event.

Exact enforcement initially covers quota-aware allocators, arenas, and buffers.
Tcmalloc sampling provides observability for allocations that have not yet been
connected to exact accounting. Exact enforcement of every `malloc` requires an
allocator-level ownership mechanism and is a separate project with explicit
performance gates.

## Assumptions

1. `TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE` limits transient query execution
   memory, not persistent tablet state or shared caches.
2. The percentage is calculated from the KQP query execution memory budget
   supplied by Memory Controller through Resource Broker, not directly from raw
   host RAM or current free RSS.
3. "All actors track allocations" initially means that actor execution can be
   attributed and residual allocations can be observed. Exact hard-limit
   enforcement initially covers managed allocations.
4. The first implementation is local to each compute node, matching existing
   CPU and memory pool settings.
5. Memory allocated under one context remains charged to that context until it
   is freed, even when the object moves to another actor or thread.
6. Actor executor threads must never synchronously wait for memory.
7. Existing Memory Controller and Resource Broker remain the node-wide memory
   authorities.

These assumptions require approval before implementation. In particular, a
requirement to enforce every process allocation in the first release changes
the task from a Workload Manager extension into an allocator project.

## Goals

- Enforce per-node resource-pool memory limits.
- Enforce per-query memory limits.
- Attribute managed allocations to database, pool, query, and category.
- Support dedicated request actors and shared service actors.
- Preserve allocation ownership across actor and thread boundaries.
- Trigger spilling or terminate a query when additional memory cannot be
  granted.
- Expose current, peak, denied, reclaimed, and residual memory metrics.
- Allow memory prioritization after hard-limit behavior is stable.
- Keep actor dispatch and allocator hot-path overhead bounded.
- Roll out incrementally under feature flags.

## Non-Goals

- Replacing Memory Controller or Resource Broker.
- Charging persistent tablet state to the query that created it.
- Charging shared caches to an arbitrary request.
- Blocking actor threads until another query releases memory.
- Immediate reclamation from lower-priority workloads.
- Exact attribution of all process memory in the first version.
- Automatically serializing local memory account pointers over Interconnect.

## Existing Implementation

### Workload Service Admission

The Workload Service maintains database and pool state, global concurrency,
queueing, and the older database CPU load threshold:

- `ydb/core/kqp/workload_service/kqp_workload_service.cpp`
- `ydb/core/kqp/workload_service/actors/pool_handlers_actors.cpp`
- `ydb/core/kqp/workload_service/common/cpu_quota_manager.cpp`

`DatabaseLoadCpuThreshold` is an admission mechanism. It estimates cluster CPU
load, reserves a default share when starting a query, and later adjusts that
estimate. It is not the CPU fair-share scheduler and is not a suitable model for
retained memory.

### CPU Scheduler

The newer CPU implementation is a per-node hierarchy:

```text
root -> database -> pool -> query -> schedulable task
```

Relevant files:

- `ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h`
- `ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.cpp`
- `ydb/core/kqp/runtime/scheduler/tree/common.h`
- `ydb/core/kqp/runtime/scheduler/tree/dynamic.h`
- `ydb/core/kqp/runtime/scheduler/tree/snapshot.h`
- `ydb/core/kqp/runtime/scheduler/kqp_schedulable_actor.cpp`
- `ydb/core/kqp/runtime/scheduler/kqp_schedulable_task.cpp`

The scheduler periodically calculates CPU fair share. Consumers pull permission
before doing CPU work and report elapsed CPU after execution. Datashard reads
use a virtual query and a token bucket.

This provides reusable hierarchy, configuration subscription, query lifetime,
and pull-client patterns. Its scalar CPU usage and delay algorithm should not be
copied for memory.

### Existing KQP Memory Limits

Pool-wide memory configuration already exists:

- `ydb/core/resource_pools/resource_pool_settings.h`
- `ydb/core/resource_pools/resource_pool_settings.cpp`
- `ydb/core/protos/workload_manager_config.proto`

`TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE` is propagated by the planner:

- `ydb/core/kqp/executer_actor/kqp_planner.cpp`
- `ydb/core/kqp/node_service/kqp_node_service.cpp`

`TKqpResourceManager` already maintains node-wide and `(database, pool)` memory
counters and rejects managed allocations above the configured cap:

- `ydb/core/kqp/rm_service/kqp_rm_service.h`
- `ydb/core/kqp/rm_service/kqp_rm_service.cpp`

MiniKQL and Arrow request additional quota through:

- `ydb/library/yql/dq/actors/compute/dq_compute_memory_quota.h`
- `yql/essentials/minikql/mkql_alloc.h`
- `yql/essentials/minikql/mkql_alloc.cpp`

Memory Controller configures the Resource Broker query-execution envelope:

- `ydb/core/memory_controller/memory_controller.cpp`
- `ydb/core/base/memory_controller_iface.h`

The current implementation is therefore a partial pool-wide memory limiter,
not only configuration plumbing.

### Existing Allocation Observability

The actor executor sets a tcmalloc sampling tag to the current actor activity:

- `ydb/library/actors/core/executor_thread.cpp`
- `ydb/library/actors/prof/tag.cpp`
- `ydb/library/actors/prof/tcmalloc.cpp`
- `ydb/core/mon_alloc/tcmalloc.cpp`

This mechanism is sampled profiling metadata. It cannot enforce exact limits.
The tag identifies actor activity type, not database, pool, query, or request.

The generic actor memory tracker is opt-in through `TTrack` and `TAlloc`:

- `ydb/library/actors/util/memory_track.h`
- `ydb/library/actors/util/memory_tracker.cpp`

It does not cover ordinary `new`, `malloc`, STL, protobuf, or third-party
allocations.

## Current Correctness Gaps

These findings are based on static inspection and should first be converted
into failing tests.

### Zero Limit

`AllocateResources()` applies a named pool only when
`MemoryPoolPercent > 0`. Configuration accepts zero as a valid percentage, but
zero therefore appears to bypass named-pool accounting rather than reject all
pool allocation.

### Stale Configuration

Each transaction stores the percentage observed when it starts. During a later
allocation it calls `SetNewLimit()` on the shared named pool. After
`ALTER RESOURCE POOL`, an old transaction can potentially restore its stale
percentage.

Pool configuration must be owned centrally by the node-level service and
updated from the Workload Service subscription. Transactions should reference a
pool account rather than redefine its limit.

### Incomplete Accounting

`ExternalMemory` is counted node-wide but is not clearly charged to the named
pool. Arbitrary actor, STL, protobuf, read-path, and helper-thread allocations
are not covered by the KQP quota.

### Missing Focused Tests

Resource Manager tests cover total query memory and concurrency, but focused
tests are needed for:

- Two named pools on one node.
- Identical pool names in different databases.
- A zero pool limit.
- An unlimited pool.
- Dynamic pool alteration with old queries running.
- Allocation rollback after Resource Broker failure.
- Pool-level spilling thresholds.
- Concurrent allocation and release in one named pool.

### First-Come-First-Served Memory

Current hard limits isolate a pool, but memory inside the node envelope remains
first-come-first-served. `RESOURCE_WEIGHT` affects CPU only.

## Proposed Architecture

```text
Memory Controller
    |
    | query execution budget
    v
Node Memory Arbiter
    |
    +-- Database Account
          |
          +-- Resource Pool Account
                |
                +-- Query Account
                      |
                      +-- Compute
                      +-- Channel
                      +-- Read
                      +-- Write
                      +-- External Source
```

Memory Controller remains responsible for physical pressure and subsystem
budgets. Resource Broker remains responsible for the query execution queue. The
new arbiter divides the KQP query execution budget among databases, pools, and
queries.

The memory arbiter should be adjacent to the CPU scheduler. It may reuse common
identity and tree registration code, but it should not initially convert the
CPU tree into a multi-resource vector algorithm.

## Memory Semantics

### Hierarchy

The initial hierarchy is:

```text
node -> database -> pool -> query
```

The node account has the query execution limit received through Resource
Broker. Pool limits come from `TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE`. Query
limits come from `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE` after that setting is
enabled.

### Outstanding Bytes

Every managed acquisition updates outstanding bytes at the query, pool,
database, and node levels. Every release updates the same original owners.

An acquisition succeeds only when all applicable hard limits permit it. The
operation must either reserve every level or roll back every partial update.

### Allocation Lifetime

Memory remains charged after the actor handler returns. It remains charged when
ownership moves to another actor or helper thread. It is released when the
allocation, page, buffer, or arena is actually returned.

### Denied Growth

Denied growth follows this sequence:

1. Ask the query to spill if the allocation category supports spilling.
2. Release unused arena pages and channel buffers.
3. Retry only from an explicit asynchronous allocation point.
4. Terminate the query if memory remains unavailable.

Actor executor threads must not synchronously wait for memory.

### Reduced Limits

When a pool or query limit is reduced below current consumption, existing
allocations remain valid. Future growth is denied. The query is asked to spill
or is eventually cancelled according to an explicit policy.

## Core Interfaces

The actor library should expose generic interfaces and must not depend on KQP
types.

```cpp
namespace NActors::NMemory {

enum class EMemoryCategory : ui8 {
    Unspecified,
    Compute,
    Channel,
    Read,
    Write,
    ExternalSource,
};

class IMemoryAccount : public TThrRefBase {
public:
    virtual bool TryAcquire(ui64 bytes) = 0;
    virtual void Release(ui64 bytes) = 0;

    virtual ui64 GetAllocated() const = 0;
    virtual ui64 GetPeakAllocated() const = 0;
    virtual bool IsOverLimit() const = 0;
};

struct TMemoryContext : public TThrRefBase {
    ui64 ContextId = 0;
    EMemoryCategory Category = EMemoryCategory::Unspecified;
    TIntrusivePtr<IMemoryAccount> Account;
};

using TMemoryContextPtr = TIntrusivePtr<TMemoryContext>;

} // namespace NActors::NMemory
```

Database, pool, and query identifiers belong to the Workload Manager
implementation. The actor runtime only needs a stable context identifier,
category, and account pointer.

## `TMemoryBoundActorBootstrapped`

### Purpose

`TMemoryBoundActorBootstrapped` provides actor-level ownership for an actor
dedicated to one request or query.

```cpp
template <class TDerived>
class TMemoryBoundActorBootstrapped
    : public TActorBootstrapped<TDerived>
{
protected:
    explicit TMemoryBoundActorBootstrapped(
        NMemory::TMemoryContextPtr memoryContext)
    {
        this->SetMemoryContext(std::move(memoryContext));
    }

    const NMemory::TMemoryContextPtr& GetMemoryContext() const {
        return this->IActor::GetMemoryContext();
    }
};
```

The base should bind a default context, expose diagnostics, and retain the
context until actor destruction. Registration with the memory arbiter should be
owned by the context or a separate RAII account handle.

### Limitation

`TActorBootstrapped` only wraps the bootstrap event. Normal events are dispatched
directly through `IActor::Receive()`. A derived bootstrapped class cannot by
itself install an allocation scope around every event.

The actor executor must install the context. The class is an ownership and
integration abstraction, not the accounting boundary.

### CPU Comparison

The CPU compute base explicitly wraps `DoExecuteImpl()` with
`StartExecution()` and `StopExecution()`. A memory equivalent must not report a
thread allocation delta around `DoExecuteImpl()`, because allocations can
survive execution and can be freed elsewhere.

The semantic mapping is:

| CPU concept | Memory concept |
| --- | --- |
| CPU demand | Requested or estimated bytes |
| CPU usage | Outstanding granted bytes |
| CPU fair share | Current memory entitlement |
| Start execution | Try to acquire bytes |
| Stop execution | Release owned bytes when actually freed |
| CPU throttle | Deny growth, spill, or cancel |
| Resume | Retry an asynchronous allocation |

The actor reports initial estimated demand, successful quota growth, releases,
peak consumption, failed allocation, and reclaimed bytes. It does not report
after every event.

## Actor Runtime Integration

### Actor Default Context

Add an optional memory context to `IActor`. Dedicated actors initialize it
through `TMemoryBoundActorBootstrapped` or another actor base.

### Event Context

Add an optional memory context to local `IEventHandle` instances. Shared actors
such as DataShard and ColumnShard must use event context because one actor serves
multiple databases, pools, and queries.

### Context Selection

The executor selects context in this order:

1. Explicit event context.
2. Actor default context.
3. Empty context.

Conceptually:

```cpp
const auto& memoryContext = ev->MemoryContext
    ? ev->MemoryContext
    : actor->GetMemoryContext();

NMemory::TMemoryContextGuard guard(memoryContext);
actor->Receive(ev);
```

The guard should cover actor handler execution and actor destruction caused by
the handler.

### Avoiding Dispatch Overhead

Do not use `dynamic_cast<IMemoryBoundActor*>` for every event. Store an optional
context pointer or compact context handle directly in `IActor`.

Adding a pointer to every `IEventHandle` also requires a benchmark. If the
overhead is too high, use a compact context identifier with a node-local
registry, or attach context only to explicitly marked events.

## Context Propagation

Events sent while a context is active inherit it by default. Explicit APIs are
required for control and background work:

```cpp
SendWithMemoryContext(recipient, event, context);
SendWithoutMemoryContext(recipient, event);
```

Forwarded and scheduled local events preserve their context. Child request
actors inherit the current context unless the caller explicitly overrides or
detaches it.

Interconnect must not serialize an arbitrary local account pointer. Existing
KQP messages carry database, pool, and query information. A receiving node
reconstructs or looks up its local account.

Helper threads and callbacks require an explicit `TMemoryContextGuard`. The
guard restores the previous context on scope exit.

Automatic propagation can incorrectly charge maintenance work triggered by a
request. Such paths must explicitly detach or replace the context.

## Exact Accounting

Exact accounting occurs at allocation and release, not at actor event
boundaries.

Initial integration points are:

- MiniKQL page pools.
- Arrow memory pools.
- DQ channel buffers.
- Scan and iterator read buffers.
- External source buffers.
- Query-owned protobuf arenas.
- DataShard transient request buffers.
- ColumnShard transient scan and processing buffers.

Requests should be batched at page or buffer granularity. The existing
incremental MiniKQL quota size is a reasonable starting point. Calling the
arbiter for every small C++ object would be too expensive.

### Quota-Aware Memory Resource

```cpp
class TQuotaMemoryResource : public std::pmr::memory_resource {
public:
    explicit TQuotaMemoryResource(
        TIntrusivePtr<NMemory::IMemoryAccount> account);

private:
    void* do_allocate(size_t bytes, size_t alignment) override;
    void do_deallocate(void* ptr, size_t bytes, size_t alignment) override;

    TIntrusivePtr<NMemory::IMemoryAccount> Account;
};
```

Existing specialized allocators can call `TryAcquire()` and `Release()`
directly instead of adopting `std::pmr`.

### Ownership Token

If actor A allocates a buffer and actor B frees it, release must update A's
original account. Free-time TLS is insufficient.

Managed buffers retain the account and charged size:

```cpp
struct TMemoryCharge {
    TIntrusivePtr<IMemoryAccount> Account;
    ui64 Bytes = 0;
};
```

Arenas can retain one account for their entire lifetime.

## Shared and Persistent Memory

Shared caches must remain Memory Controller consumers. Charging a reusable
cache to the first request that populated it creates incorrect long-lived
ownership and makes resource-pool behavior depend on request order.

Transient request buffers in DataShard and ColumnShard can be charged to the
request context. Persistent tablet structures, indexes, memtables, and caches
need subsystem-level memory policies rather than query-level accounting.

Table type can be recorded as an allocation category or diagnostic dimension.
It should not replace database, pool, and query ownership.

## Pool and Query Configuration

The node-level memory arbiter owns current pool configuration. Workload Service
subscriptions update pool accounts directly. `TTxState` references an account
and does not update the pool's configured percentage.

The initial hard limits are:

```text
node limit  = Memory Controller query execution budget
pool limit  = node limit * TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE / 100
query limit = selected base * QUERY_MEMORY_LIMIT_PERCENT_PER_NODE / 100
```

The base for the per-query percentage remains an open decision. It may be node
capacity or effective pool capacity.

`-1` means no limit at that hierarchy level. `0` means no allocation can be
granted at that level.

## Memory Priority

### First Version

Implement hard pool and query limits without weighted redistribution. This is
the smallest correctness step and matches existing documented failure
semantics.

### Weighted Entitlement

A later version can calculate weighted entitlements among active pools. Unused
entitlement may be borrowed.

When a pool's entitlement falls below its current allocation:

- Existing memory remains valid.
- New allocations are denied.
- Queries are asked to spill.
- Queries may be cancelled if they cannot converge.

Immediate guarantees require reserving unused memory or cancelling borrowers.
Memory priority therefore cannot behave exactly like CPU throttling.

### Configuration Choice

Current documentation says `RESOURCE_WEIGHT` affects CPU only. Applying it to
memory creates a behavioral change.

Option A is to make `RESOURCE_WEIGHT` apply to every scheduler resource,
matching a multi-resource DRF model.

Option B is to add a memory-specific weight, preserving existing CPU behavior
but increasing configuration complexity.

This decision must be explicit and documented before weighted memory admission
is implemented.

## Why Not Extend CPU HDRF Immediately

The current CPU scheduler stores scalar CPU demand, usage, limit, guarantee,
and fair share. Converting it to multi-resource HDRF affects all calculations,
tests, counters, and consumers.

Memory also has different lifecycle and preemption semantics. A separate memory
arbiter beside the CPU scheduler is the smaller and safer implementation.

A future shared tree can provide common identities and configuration while
resource-specific policies continue to calculate CPU rate and memory occupancy
separately. Full vector HDRF should be driven by an explicit requirement for
combined CPU and RAM dominant-share fairness.

## Tcmalloc Integration

Tcmalloc user data can be extended with a bounded context tag for pool or
allocation category. It is useful for:

- Finding major residual allocation sources.
- Comparing managed memory with sampled total memory.
- Prioritizing allocator integration work.
- Detecting coverage regressions.

It must not enforce hard limits because heap profiles are sampled and delayed.

Per-query tcmalloc labels should be avoided because query cardinality is
unbounded. Dynamic counters should use node and pool dimensions only when
cardinality is controlled. Per-query information belongs in query statistics
or diagnostic snapshots.

## Exact Global Allocation Tracking

Exact tracking of every ordinary allocation requires the allocation to remember
its original account until deallocation. Possible implementations are:

- A tcmalloc modification that stores an ownership identifier per allocation.
- A global allocation side table.
- An allocation header added by an interposing allocator.
- Migration to an allocator with exact memory tags.

All options affect allocator hot paths, memory overhead, or allocator
compatibility. Cross-thread frees, realloc, aligned allocation, sampled
allocations, allocator caches, and recursion during tracker allocation require
special handling.

This should be a separate prototype behind a build or runtime flag. It should
not block cooperative Workload Manager enforcement.

## Metrics

Node-level metrics:

```text
Memory/LimitBytes
Memory/AllocatedBytes
Memory/ManagedBytes
Memory/ResidualEstimatedBytes
Memory/DeniedBytes
Memory/DeniedRequests
```

Pool-level metrics:

```text
Memory/LimitBytes
Memory/EntitlementBytes
Memory/AllocatedBytes
Memory/PeakAllocatedBytes
Memory/ReclaimedBytes
Memory/SpilledBytes
Memory/DeniedRequests
Memory/OverLimit
Memory/Satisfaction
```

Query-level data belongs in query statistics:

```text
MemoryAllocatedBytes
MemoryPeakBytes
MemoryDeniedBytes
MemorySpilledBytes
MemoryLimitBytes
MemoryPoolId
```

## Implementation Plan

### Phase 0: Approve Semantics

Define the percentage base, zero-limit behavior, error status, spilling policy,
configuration update behavior, default pool behavior, cache exclusions, and
weight semantics.

Verification: approved design and Tracker ticket with decisions recorded.

### Phase 1: Stabilize Existing Pool Limits

Add focused tests for current named-pool accounting. Fix zero-limit behavior,
stale transaction configuration, rollback, pool identity, and pool metrics.

Move pool configuration ownership out of `TTxState`. Keep the existing KQP
allocation path and Resource Broker integration.

Verification:

```bash
ya make ydb/core/kqp/rm_service -rA
ya make ydb/core/kqp/workload_service/ut -rA
```

### Phase 2: Extract the Memory Arbiter

Extract node, database, pool, and query accounting from
`TKqpResourceManager`. Provide atomic `TryAcquire()` and `Release()` operations,
account snapshots, and limit updates.

Resource Manager delegates KQP allocation decisions to the arbiter. Resource
Broker remains the node-level admission authority.

Verification: unit tests for hierarchy, rollback, concurrent allocation,
dynamic limits, and account cleanup.

### Phase 3: Add Per-Query Limits

Enable `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE`. Create a query account at KQP
query registration and propagate its identity to execution nodes.

Enforce query and pool limits in one allocation operation. Connect denied
growth to spilling and documented query failure.

Verification: local and distributed KQP tests with multiple queries in one
pool.

### Phase 4: Complete Cooperative KQP Coverage

Connect MiniKQL, Arrow, channels, external sources, scans, and query-owned
arenas. Classify each allocation by category.

Track managed bytes and compare them to current Resource Manager accounting and
process allocator statistics.

Verification: spilling, channel, scan, external source, and cancellation tests.

### Phase 5: Add Actor Memory Context

Add an optional default context to `IActor`. Implement
`TMemoryBoundActorBootstrapped` and a KQP-specific memory-bound compute actor
base. Install the context in the actor executor.

Benchmark actor dispatch before adding event-level context.

Verification:

```bash
ya make ydb/library/actors/core -rA
ya make ydb/core/kqp/runtime -rA
```

### Phase 6: Add Event Context for Shared Actors

Propagate context through local send, forward, schedule, and child actor
registration. Add explicit detach and override APIs. Reconstruct context at KQP
Interconnect boundaries.

Integrate transient DataShard and ColumnShard request allocations.

Verification: local, scheduled, forwarded, cross-node, shared-actor, and helper
thread tests.

### Phase 7: Add Sampled Residual Attribution

Extend tcmalloc sampling tags with bounded resource-pool or category context.
Expose managed versus sampled residual diagnostics.

Verification: controlled allocation workloads with known managed and unmanaged
fractions.

### Phase 8: Add Weighted Memory Admission

After weight semantics are approved, calculate pool entitlements, allow
borrowing, deny growth above reduced entitlement, and request spilling from
borrowers.

Verification: competing pools with equal and unequal weights, borrowing,
returning demand, and non-reclaiming queries.

### Phase 9: Evaluate Full Allocator Tracking

Prototype allocator-level ownership behind a flag. Implement all allocation and
deallocation forms required by the selected allocator. Measure overhead and
accuracy.

Verification: allocator microbenchmarks, actor throughput, production-like
queries, cross-thread frees, and RSS comparison.

## Test Plan

### Memory Arbiter Unit Tests

- Zero, unlimited, and fractional limits.
- Query, pool, database, and node ancestor enforcement.
- Complete rollback after failed reservation.
- Release through the original account.
- Concurrent acquisition and release.
- Limit increase and decrease.
- Account removal with outstanding allocations.
- Overflow and saturation behavior.

### Resource Manager Tests

- Two named pools on one node.
- Same pool name in different databases.
- Multiple queries in one pool.
- Dynamic `ALTER RESOURCE POOL` with old queries running.
- Resource Broker rejection after a pool reservation.
- Query destruction and cancellation cleanup.
- Spilling threshold at node and pool levels.
- External memory pool accounting.

### Actor Context Tests

- Dedicated actor default context.
- Event context overrides actor context.
- Forwarded event preserves context.
- Scheduled event preserves context.
- Explicit detach clears context.
- Child actor inheritance.
- Shared actor handles alternating contexts.
- Helper thread guard restores previous context.
- Actor destruction releases context references.
- Allocation in one actor and release in another.

### Integration Tests

- Local KQP query pool limit.
- Distributed KQP query pool limit.
- Per-query limit inside a larger pool.
- Multiple pools competing on one node.
- DataShard read buffers.
- ColumnShard scan buffers.
- DQ channel memory.
- External source memory.
- Spilling and non-spilling execution.
- Limit alteration during execution.
- Default and serverless database behavior.

## Benchmark Plan

### Actor Runtime

Measure event throughput and latency for:

- No memory context support.
- Actor default context with an empty account.
- Actor default context with an active account.
- Event context propagation.
- Context override and detach.

### Allocation

Measure allocation and deallocation for sizes from 16 bytes through large
pages. Include same-thread and cross-thread frees, realloc, aligned allocation,
arena growth, and contention on one pool account.

### Workloads

Run:

- TPC-C with competing analytical scans.
- OLAP scans in multiple pools.
- Equal and unequal pool weights.
- Queries with and without spilling.
- Multiple databases sharing compute nodes.
- DataShard-heavy reads.
- ColumnShard-heavy scans.

Record peak RSS, managed bytes, sampled residual bytes, spill volume, rejected
allocations, throughput, and p50/p95/p99 latency.

### Suggested Performance Gates

Suggested gates for discussion:

- Less than 2% actor throughput loss for actor default context.
- Less than 5% p99 regression for representative queries.
- No per-small-allocation global lock in the cooperative path.
- Bounded memory overhead per actor and event.
- Exact managed accounting within allocator page granularity.

The final gates must be agreed before implementation.

## Rollout Plan

1. Ship pool and query accounting in observe-only mode.
2. Compare managed accounting with Resource Manager and allocator statistics.
3. Enable existing pool hard limits behind a feature flag.
4. Enable per-query hard limits.
5. Enable additional KQP allocation categories incrementally.
6. Enable actor and event context propagation after benchmarks pass.
7. Enable transient DataShard and ColumnShard charging.
8. Enable weighted memory admission after production validation.
9. Evaluate allocator-wide exact tracking independently.

Every rollout stage needs counters for denied allocation, spills, cancellation,
accounting mismatch, and time spent in the memory arbiter.

## Alternatives

### Report Per-Event Allocation Delta

Rejected for enforcement. Thread or process allocation deltas do not represent
live memory owned by an actor. Allocations survive events, migrate between
actors, and may be freed by another thread.

It may be used as a diagnostic approximation only if the allocator exposes
cheap thread-local cumulative allocation counters.

### Copy the CPU Scheduler

Rejected. CPU usage is a renewable rate and can be throttled. Memory is
outstanding occupancy and cannot be resumed without a release or reclamation
event.

### Convert CPU Scheduler to Multi-Resource HDRF First

Deferred. It is broader than required for hard memory limits and introduces
substantial risk to existing CPU behavior. Common tree identity can be extracted
without changing CPU calculations.

### Use Tcmalloc Heap Profiles for Enforcement

Rejected. Heap profiles are sampled, delayed, and unsuitable for exact hard
limits.

### Add Custom Allocators to Every Actor Class

Rejected as the primary mechanism. It requires pervasive source changes and
still misses third-party and ordinary allocations. Quota-aware arenas and
buffers should be integrated at dominant allocation points instead.

### Charge All Shared Caches to Requests

Rejected. Cache lifetime and reuse do not follow request ownership. Shared
caches remain subsystem consumers under Memory Controller.

## Risks

### Double Accounting

The arbiter, KQP Resource Manager, Resource Broker, and Memory Controller must
have clearly separated responsibilities. The same bytes must not be reserved
twice against independent node totals.

### Context Leakage

Automatic propagation can charge background maintenance to the initiating
request. Explicit detach APIs and tests are required.

### High Cardinality

Per-query dynamic counters and tcmalloc tags can grow without bounds. Query
details must use query statistics or bounded diagnostic snapshots.

### Non-Reclaimable Borrowing

Weighted memory borrowing can prevent a higher-priority pool from immediately
receiving its entitlement. Guarantees require reservation or cancellation.

### Hot-Path Contention

Updating ancestor atomics or locks for every small allocation can become a
bottleneck. Allocations must be batched and local counters considered where
appropriate.

### Context Lifetime

Allocations may survive query or actor completion. Account objects need stable
lifetime and a closed state that accepts releases but rejects new acquisitions.

### Allocator Compatibility

Tcmalloc, alternative allocators, sanitizers, and test builds may expose
different hooks and size semantics. Cooperative accounting must not depend on a
single allocator.

## Success Criteria

- A pool cannot exceed its configured managed-memory hard limit except for
  documented allocation granularity.
- A query cannot exceed its configured managed-memory hard limit.
- Node-wide query allocations remain within the Memory Controller and Resource
  Broker envelope.
- Failed allocation leaves every account unchanged.
- Allocation release updates the original owner regardless of current actor or
  thread.
- Dynamic configuration cannot be overwritten by stale transactions.
- Shared actors correctly attribute alternating request contexts.
- Spilling is attempted before terminating eligible queries.
- Metrics explain pool and query allocation failures.
- Actor and allocation benchmarks satisfy agreed performance gates.
- Observe-only residual metrics quantify untracked allocation coverage.

## Open Questions

### Product Semantics

1. Is `TOTAL_MEMORY_LIMIT_PERCENT_PER_NODE` based on raw process memory, Memory
   Controller's query execution budget, or currently available memory?
2. Does `QUERY_MEMORY_LIMIT_PERCENT_PER_NODE` apply relative to node capacity or
   effective pool capacity?
3. Does a zero pool limit reject a query before execution or on its first
   managed allocation?
4. Which status must be returned for initial and incremental memory denial?
5. How long may an over-limit query attempt spilling before cancellation?
6. What happens to running queries after their pool limit is reduced?
7. Is the default pool allowed to have memory limits?
8. Are serverless databases subject to the same per-node percentage semantics?

### Priority and Guarantees

9. Should `RESOURCE_WEIGHT` apply to memory as well as CPU?
10. If not, should a separate memory weight be introduced?
11. Are memory guarantees required, or only limits and best-effort priority?
12. May idle guaranteed memory be borrowed by other pools?
13. Can borrowers be cancelled when the guaranteed owner returns?
14. Is prioritization required across databases sharing a node?
15. Where are database-level memory weights or guarantees configured?

### Accounting Scope

16. Which transient DataShard allocations are charged to a query?
17. Which transient ColumnShard allocations are charged to a query?
18. Are DQ channel buffers part of pool and query limits?
19. Is external source memory included in the same limit?
20. Are query compilation and plan caches included?
21. Are event payloads charged to the sender, receiver, or request context?
22. Are shared caches always excluded from Workload Manager accounting?
23. How are allocations retained after query completion classified?
24. Is table type an enforcement dimension or only a diagnostic category?

### Actor Runtime

25. Is adding a context pointer to every `IActor` acceptable?
26. Is adding context to every local `IEventHandle` acceptable?
27. Should context propagation be automatic or opt-in by event type?
28. Which sends must detach context by default?
29. How is context propagated through actor coroutines and helper executors?
30. Should child actor registration inherit the current event context or only
    the parent actor default context?
31. How are local event contexts reconstructed after Interconnect delivery?

### Allocator Integration

32. Is exact enforcement of ordinary `malloc` required, or is cooperative
    coverage sufficient when residual memory is measured?
33. Which allocator configurations must support exact context attribution?
34. What actor dispatch overhead is acceptable?
35. What allocation throughput and memory overhead are acceptable?
36. Is a tcmalloc modification acceptable for YDB builds?
37. How should realloc transfer ownership and adjust quota atomically?
38. How should allocator metadata and tracker allocations avoid recursion?

### Operations and Rollout

39. Which feature flags control observation, pool enforcement, query
    enforcement, context propagation, and weighted admission?
40. Which counters and system views are required before enforcement is enabled?
41. How is accounting mismatch between managed bytes and RSS alerted?
42. What is the emergency bypass for administrators?
43. What production workloads define acceptable performance and correctness?
44. What compatibility behavior is required during rolling upgrades?
