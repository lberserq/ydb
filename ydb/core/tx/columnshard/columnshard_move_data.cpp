#include "columnshard_impl.h"

#include "engines/column_engine_logs.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/data_accessor/request.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

namespace {

THashSet<ui32> RequestedGroups(const NKikimrTabletBase::TEvMoveData& record) {
    THashSet<ui32> groups;
    for (const auto groupId : record.GetGroups()) {
        groups.emplace(groupId);
    }
    return groups;
}

// Same contract as keyvalue and blob_depot: a group that is still the latest entry keeps taking writes, so the move could never converge.
std::optional<ui32> FindLiveGroup(const TTabletStorageInfo& info, const THashSet<ui32>& groups) noexcept {
    if (groups.empty()) {
        return std::nullopt;
    }
    for (const auto& channel : info.Channels) {
        const auto* latest = channel.LatestEntry();
        if (latest && groups.contains(latest->GroupID)) {
            return latest->GroupID;
        }
    }
    return std::nullopt;
}

void RefuseMoveData(const TActorId& sender, const ui64 tabletId, const ui32 liveGroup, const TActorContext& ctx) {
    const TString reason = TStringBuilder() << "group " << liveGroup << " is still the latest history entry at tablet " << tabletId;
    YDB_LOG_WARN("MoveData refused", {"tabletId", tabletId}, {"reason", reason});
    ctx.Send(sender, new TEvTablet::TEvMoveDataResponse(tabletId, NKikimrTabletBase::TEvMoveDataResponse::ErrorGroupIdMismatch, reason));
}

// Outside a session the set is already empty, so a first request is always a change; returns whether the target set changed.
bool MergeTargetGroups(TMoveDataState& state, const THashSet<ui32>& requested) {
    bool changed = !state.Active;
    for (const auto groupId : requested) {
        changed |= state.TargetGroups.emplace(groupId).second;
    }
    return changed;
}

class TMoveDataMetadataResultProcessor: public NOlap::IMetadataAccessorResultProcessor {
    const TActorId Driver;
    const ui64 RequestId;

    void DoApplyResult(
        NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult>&& result, NOlap::TColumnEngineForLogs&) override {
        TActivationContext::Send(Driver, std::make_unique<TEvPrivate::TEvMoveDataMetadataResult>(RequestId, std::move(result)));
    }

    bool DoIsMoveData() const noexcept override {
        return true;
    }

public:
    TMoveDataMetadataResultProcessor(const TActorId driver, const ui64 requestId) noexcept
        : Driver(driver)
        , RequestId(requestId)
    {
    }
};

std::vector<ui32> GetBlobGroupsForLog(const std::vector<NOlap::TUnifiedBlobId>& blobIds) {
    std::vector<ui32> result;
    for (const auto& blobId : blobIds) {
        result.emplace_back(blobId.GetDsGroup());
    }
    SortUnique(result);
    return result;
}

}   // namespace

void TMoveDataMetadataScan::AddCandidate(const NOlap::TPortionInfo& portion, const NOlap::TVersionedIndex& index) {
    const TString tier = portion.GetTierNameDef(NOlap::IStoragesManager::DefaultStorageId);
    if (tier != NOlap::IStoragesManager::DefaultStorageId) {
        const auto schema = portion.GetSchema(index);
        const auto& indexInfo = schema->GetIndexInfo();
        if (!AnyOf(indexInfo.GetEntityIds(), [&](const ui32 entityId) {
                return indexInfo.GetEntityStorageId(entityId, tier) == NOlap::IStoragesManager::DefaultStorageId;
            })) {
            return;
        }
    }
    Candidates.emplace_back(portion.GetPathId(), portion.GetPortionId());
}

std::vector<TMoveDataMetadataScan::TPortionAddress> TMoveDataMetadataScan::TakePendingPortions() noexcept {
    std::vector<TPortionAddress> result;
    result.swap(Pending);
    return result;
}

bool TMoveDataMetadataScan::HasBlobInGroups(const std::vector<NOlap::TUnifiedBlobId>& blobIds, const THashSet<ui32>& groups) noexcept {
    return AnyOf(blobIds, [&groups](const NOlap::TUnifiedBlobId& blobId) noexcept {
        return groups.contains(blobId.GetDsGroup());
    });
}

void TColumnShard::Handle(TEvTablet::TEvMoveData::TPtr& ev, const TActorContext& ctx) {
    if (!AppData()->FeatureFlags.GetEnableColumnshardMoveData()) {
        TTabletExecutedFlat::Handle(ev);
        return;
    }
    const THashSet<ui32> requested = RequestedGroups(ev->Get()->Record);
    if (const auto liveGroup = FindLiveGroup(*Info(), requested)) {
        RefuseMoveData(ev->Sender, TabletID(), *liveGroup, ctx);
        return;
    }
    MoveDataState.HiveSender = ev->Sender;
    const bool changed = MergeTargetGroups(MoveDataState, requested);
    // Before Active is set, so an active session always has a driver to hand the gate to.
    StartMoveDataDriver(ctx);
    if (!MoveDataState.Active) {
        MoveDataState.Active = true;
        Counters.GetCSCounters().OnMoveDataStarted();
        // The vacuum leg belongs to the executor; everything else to the driver.
        Executor()->StartMoveDataVacuumFromOwner();
    }
    YDB_LOG_INFO("MoveData requested", {"tabletId", TabletID()}, {"groups", MoveDataState.TargetGroups.size()}, {"changed", changed});
    // Marked synchronously: a gate check already queued must not answer for a stale target set.
    MoveDataState.TargetsChanged |= changed;
    ctx.Send(MoveDataDriverId, new TEvPrivate::TEvMoveDataPoke());
}

void TMoveDataDriver::RestartMoveData() {
    AFL_VERIFY(Self->MoveDataState.Active);
    Self->MoveDataState.TargetsChanged = false;
    Self->MoveDataState.ReportedRejections = 0;
    RejectedPortions = 0;
    PendingRequestId = 0;
    RetryMetadataAfter = TInstant::Zero();
    MetadataScan = TMoveDataMetadataScan();
    if (!Self->HasIndex()) {
        return;
    }
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    index.StopMoveData();
    if (Self->MoveDataState.TargetGroups.empty()) {
        return;
    }
    index.StartMoveData();
    for (const auto& [_, granule] : index.GetTables()) {
        for (const auto& [_, portion] : granule->GetPortions()) {
            MetadataScan.AddCandidate(*portion, index.GetVersionedIndex());
        }
        for (const auto& [_, portion] : granule->GetInsertedPortions()) {
            MetadataScan.AddCandidate(*portion, index.GetVersionedIndex());
        }
    }
}

void TColumnShard::StartMoveDataDriver(const TActorContext& ctx) {
    if (!!MoveDataDriverId) {
        return;
    }
    MoveDataDriverId = ctx.RegisterWithSameMailbox(new TMoveDataDriver(this, TabletActivityImpl));
}

void TColumnShard::StopMoveDataDriver(const TActorContext& ctx) {
    if (!MoveDataDriverId) {
        return;
    }
    ctx.Send(MoveDataDriverId, new TEvents::TEvPoison());
    MoveDataDriverId = {};
}

void TColumnShard::MoveDataCompleted(const TActorContext& ctx) {
    if (!MoveDataState.Active) {
        return;
    }
    MoveDataState.VacuumCompleted = true;
    // The driver owns the gate; hand it the news rather than deciding here.
    ctx.Send(MoveDataDriverId, new TEvPrivate::TEvMoveDataPoke());
}

NOlap::NActualizer::TMoveDataQueueSizes TColumnShard::GetMoveDataQueueSizes() const {
    if (!HasIndex()) {
        return {};
    }
    return GetIndexAs<NOlap::TColumnEngineForLogs>().GetMoveDataQueueSizes();
}

void TColumnShard::CheckMoveDataGate(const TActorContext& ctx, const NOlap::NActualizer::TMoveDataQueueSizes& queues) {
    // The driver checked session activity and applied target changes in this same mailbox turn.
    Counters.GetCSCounters().OnMoveDataGateChecked();

    Counters.GetCSCounters().OnMoveDataQueues(queues.Pending, queues.ConfirmedToMove, queues.InFlight, queues.Uncommitted, queues.Retired);
    if (queues.Rejected > MoveDataState.ReportedRejections) {
        Counters.GetCSCounters().OnMoveDataPortionsRejected(queues.Rejected - MoveDataState.ReportedRejections);
        MoveDataState.ReportedRejections = queues.Rejected;
    }
    if (!MoveDataState.VacuumCompleted) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByVacuum();
        return;
    }
    if (queues.GetTotal() != 0) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByPortions();
        if (queues.Uncommitted) {
            YDB_LOG_INFO("MoveData gate waits for uncommitted writes", {"tabletId", TabletID()}, {"uncommitted", queues.Uncommitted});
        }
        return;
    }
    // A selected target portion still in the granule has not reached the GC queues.
    if (queues.Retired != 0) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByCleanup();
        YDB_LOG_INFO("MoveData gate waits for cleanup", {"tabletId", TabletID()}, {"retired", queues.Retired});
        return;
    }
    const auto& defaultOperator = GetStoragesManager()->GetDefaultOperator();
    if (!defaultOperator->HasCollectedBeforeCurrentGeneration()) {
        Counters.GetCSCounters().OnMoveDataGateBlockedByFirstGCRound();
        YDB_LOG_INFO("MoveData gate waits for the first GC round", {"tabletId", TabletID()});
        return;
    }
    if (defaultOperator->HasBlobsForGroups(MoveDataState.TargetGroups)) {
        // Same wait either way, but a shared or borrowed link is not ours to collect, so it gets its own sensor.
        const auto& sharedBlobs = defaultOperator->GetSharedBlobs();
        if (sharedBlobs->HasBlobsForGroups(MoveDataState.TargetGroups)) {
            Counters.GetCSCounters().OnMoveDataGateBlockedByShared();
            YDB_LOG_INFO("MoveData gate waits for shared blobs", {"tabletId", TabletID()});
        } else {
            Counters.GetCSCounters().OnMoveDataGateBlockedByGC();
            YDB_LOG_INFO("MoveData gate waits for pending GC", {"tabletId", TabletID()});
        }
        return;
    }
    YDB_LOG_INFO("MoveData gate passed", {"tabletId", TabletID()});

    if (HasIndex()) {
        MutableIndexAs<NOlap::TColumnEngineForLogs>().StopMoveData();
    }
    // The boot-time CutHistory scan finds drained intervals by itself, so nothing needs persisting before Success.
    ctx.Send(MoveDataState.HiveSender, new TEvTablet::TEvMoveDataResponse(TabletID(), NKikimrTabletBase::TEvMoveDataResponse::Success));
    Counters.GetCSCounters().OnMoveDataFinished();
    MoveDataState = TMoveDataState{};
    StopMoveDataDriver(ctx);
}

void TMoveDataDriver::SubmitMetadataBatch(const TActorContext& ctx) {
    if (PendingRequestId || !MetadataScan.GetPendingCount() || ctx.Now() < RetryMetadataAfter) {
        return;
    }
    // Pending candidates were captured from this index, which lives until tablet shutdown.
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    auto request = MetadataScan.BuildRequest(index.GetVersionedIndex().GetLastSchema(),
        NYDBTest::TControllers::GetColumnShardController()->GetMetadataRequestSoftMemoryLimit(),
        [&](const TMoveDataMetadataScan::TPortionAddress& address) {
            const auto granule = index.GetGranuleOptional(address.first);
            return granule ? granule->GetPortionOptional(address.second, false) : nullptr;
        });
    if (request->IsEmpty()) {
        return;
    }
    PendingRequestId = ++NextRequestId;
    Self->StartMetadataRequests(
        { NOlap::TCSMetadataRequest(request, std::make_shared<TMoveDataMetadataResultProcessor>(SelfId(), PendingRequestId)) },
        Self->MoveDataTaskSubscription, Self->MoveDataMetadataRequestsInFlight);
}

void TColumnShard::SetupMoveDataRewrites() {
    // The active driver calls this only when the index has selected portions ready to rewrite.
    const ui64 memoryUsageLimit = AppDataVerified().ColumnShardConfig.GetTieringsMemoryLimit();
    std::vector<std::shared_ptr<NOlap::TTTLColumnEngineChanges>> indexChanges = TablesManager.MutablePrimaryIndex().StartTtl(
        {}, DataLocksManager, memoryUsageLimit, NOlap::NActualizer::EActualizationScope::MoveDataOnly);
    if (indexChanges.empty()) {
        return;
    }
    StartTtlChanges(std::move(indexChanges));
}

void TMoveDataDriver::StartAndCheckGate(const TActorContext& ctx) {
    if (Self->MoveDataDriverId != SelfId()) {
        PassAway();
        return;
    }
    if (!Self->MoveDataState.Active) {
        return;
    }
    if (Self->MoveDataState.TargetsChanged) {
        RestartMoveData();
    }
    SubmitMetadataBatch(ctx);
    auto queues = Self->GetMoveDataQueueSizes();
    queues.Pending = MetadataScan.GetPendingCount();
    queues.Rejected = RejectedPortions;
    if (queues.ConfirmedToMove) {
        Self->SetupMoveDataRewrites();
    }
    Self->CheckMoveDataGate(ctx, queues);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataMetadataResult::TPtr& ev, const TActorContext& ctx) {
    if (!IsOwnerAlive()) {
        PassAway();
        return;
    }
    if (Self->MoveDataDriverId != SelfId()) {
        PassAway();
        return;
    }
    if (!Self->MoveDataState.Active) {
        return;
    }
    if (Self->MoveDataState.TargetsChanged) {
        StartAndCheckGate(ctx);
        return;
    }
    if (!PendingRequestId || ev->Get()->RequestId != PendingRequestId) {
        return;
    }
    PendingRequestId = 0;
    bool retry = false;
    const auto& result = ev->Get()->Result.GetValue();
    auto& index = Self->MutableIndexAs<NOlap::TColumnEngineForLogs>();
    for (const auto& address : MetadataScan.TakePendingPortions()) {
        const auto granule = index.GetGranuleOptional(address.first);
        const auto portion = granule ? granule->GetPortionOptional(address.second, false) : nullptr;
        // Absence follows cleanup Complete, which has published the old blobs to the GC queues.
        if (!portion) {
            continue;
        }
        const auto it = result.GetPortions().find(address.second);
        if (it == result.GetPortions().end()) {
            MetadataScan.RetryPortion(address);
            retry = true;
            continue;
        }
        const auto& accessor = *it->second;
        AFL_VERIFY(accessor.GetPortionInfo().GetPathId() == address.first);
        if (TMoveDataMetadataScan::HasBlobInGroups(accessor.GetBlobIds(), Self->MoveDataState.TargetGroups)) {
            // Resolve lifecycle from the current index: the portion may have committed or retired during the request.
            granule->AddPortionToMove(*portion);
        } else {
            ++RejectedPortions;
            YDB_LOG_DEBUG_COMP(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION, "",
                {"event", "move_data_portion_rejected"},
                {"portionId", address.second},
                {"blobs", JoinSeq(",", GetBlobGroupsForLog(accessor.GetBlobIds()))},
                {"targets", JoinSeq(",", Self->MoveDataState.TargetGroups)});
        }
    }
    if (retry) {
        YDB_LOG_WARN("MoveData metadata batch will retry", {"tabletId", Self->TabletID()}, {"errors", result.HasErrors()});
    }
    RetryMetadataAfter = retry ? ctx.Now() + Cadence : TInstant::Zero();
    StartAndCheckGate(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataWakeup::TPtr&, const TActorContext& ctx) {
    if (!IsOwnerAlive()) {
        PassAway();
        return;
    }
    StartAndCheckGate(ctx);
    ScheduleWakeup(ctx);
}

void TMoveDataDriver::Handle(TEvPrivate::TEvMoveDataPoke::TPtr&, const TActorContext& ctx) {
    if (!IsOwnerAlive()) {
        PassAway();
        return;
    }
    StartAndCheckGate(ctx);
}

}   // namespace NKikimr::NColumnShard
