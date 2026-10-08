#pragma once

#include "columnshard_private_events.h"

#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/queue_sizes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <deque>
#include <functional>

namespace NKikimr::NColumnShard {

class TColumnShard;

// Candidate ids exist only to feed metadata-cache requests; completed batches relinquish their ids.
class TMoveDataMetadataScan {
public:
    using TPortionAddress = std::pair<NOlap::TInternalPathId, ui64>;
    using TPortionLookup = std::function<NOlap::TPortionInfo::TPtr(const TPortionAddress&)>;

private:
    std::deque<TPortionAddress> Candidates;
    std::vector<TPortionAddress> Pending;

public:
    void AddCandidate(const NOlap::TPortionInfo& portion, const NOlap::TVersionedIndex& index);

    void RetryPortion(const TPortionAddress& address) {
        Candidates.push_back(address);
    }

    ui64 GetPendingCount() const {
        return Candidates.size() + Pending.size();
    }

    std::shared_ptr<NOlap::TDataAccessorsRequest> BuildRequest(
        const NOlap::ISnapshotSchema::TPtr& schema, ui64 memorySoftLimit, const TPortionLookup& lookup);
    std::vector<TPortionAddress> TakePendingPortions();

    static bool HasBlobInGroups(const std::vector<NOlap::TUnifiedBlobId>& blobIds, const THashSet<ui32>& groups);
};

// Stateless v1: no persistence; on restart Hive re-sends TEvMoveData.
struct TMoveDataState {
    TActorId HiveSender;
    THashSet<ui32> TargetGroups;
    bool Active = false;
    // Set by the executor's MoveDataCompleted(): vacuum done, the blob gates still pending.
    bool VacuumCompleted = false;
    // The driver's rejection count is cumulative; track what was reported to keep the sensor a rate.
    ui64 ReportedRejections = 0;
    // The driver restarts the actualizer for the new set before any gate check may pass.
    bool TargetsChanged = false;
};

// Owns candidate scanning, metadata classification, rewrites and completion; shares the tablet's mailbox.
class TMoveDataDriver: public TActorBootstrapped<TMoveDataDriver> {
private:
    TColumnShard* Self;
    // Dropped to zero by TColumnShard::Die before it poisons us; an event queued in between must not touch Self.
    const std::shared_ptr<TAtomicCounter> TabletActivity;
    TMoveDataMetadataScan MetadataScan;
    ui64 NextRequestId = 0;
    ui64 PendingRequestId = 0;
    ui64 RejectedPortions = 0;
    TInstant RetryMetadataAfter;
    // Periodic fallback interval; pokes run a turn sooner.
    static constexpr TDuration Cadence = TDuration::Seconds(5);

    void ScheduleWakeup(const TActorContext& ctx) {
        ctx.Schedule(Cadence, new TEvPrivate::TEvMoveDataWakeup());
    }

    bool IsOwnerAlive() const {
        return TabletActivity->Val() != 0;
    }

    void StartAndCheckGate(const TActorContext& ctx);
    void RestartMoveData();
    void SubmitMetadataBatch(const TActorContext& ctx);

    void Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx);
    void Handle(TEvPrivate::TEvMoveDataMetadataResult::TPtr& ev, const TActorContext& ctx);

public:
    TMoveDataDriver(TColumnShard* self, const std::shared_ptr<TAtomicCounter>& tabletActivity)
        : Self(self)
        , TabletActivity(tabletActivity)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateWork);
        // The tablet pokes after registering; this is the periodic fallback.
        ScheduleWakeup(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvMoveDataWakeup, Handle);
            HFunc(TEvPrivate::TEvMoveDataPoke, Handle);
            HFunc(TEvPrivate::TEvMoveDataMetadataResult, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                break;
        }
    }
};

}   // namespace NKikimr::NColumnShard
