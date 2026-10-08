#include "move.h"

#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/construction/context.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NActualizer {

void TMoveDataActualizer::RemoveFromActiveQueue(ui64 portionId) {
    auto it = PortionAddress.find(portionId);
    if (it == PortionAddress.end()) {
        return;
    }
    auto itAddr = PortionsToMove.find(it->second);
    AFL_VERIFY(itAddr != PortionsToMove.end());
    AFL_VERIFY(itAddr->second.erase(portionId));
    if (itAddr->second.empty()) {
        PortionsToMove.erase(itAddr);
    }
    PortionAddress.erase(it);
}

void TMoveDataActualizer::QueueSelectedPortion(const TPortionInfo& info) {
    const ui64 portionId = info.GetPortionId();
    if (info.HasRemoveSnapshot()) {
        RetiredPortionIds.emplace(portionId);
        return;
    }
    if (!info.IsCommitted()) {
        UncommittedPortionIds.emplace(portionId);
        return;
    }
    const auto schema = info.GetSchema(VersionedIndex);
    const TString tierName = info.GetTierNameDef(IStoragesManager::DefaultStorageId);
    auto readStorages = schema->GetIndexInfo().GetUsedStorageIds(tierName);
    auto writeStorages = readStorages;
    TRWAddress address(std::move(readStorages), std::move(writeStorages));
    AFL_VERIFY(PortionsToMove[address].emplace(portionId).second);
    AFL_VERIFY(PortionAddress.emplace(portionId, std::move(address)).second);
}

void TMoveDataActualizer::AddPortionToMove(const TPortionInfo& info) {
    if (SelectedPortionIds.emplace(info.GetPortionId()).second) {
        QueueSelectedPortion(info);
    }
}

void TMoveDataActualizer::DoAddPortion(const TPortionInfo& info, const TAddExternalContext& /*context*/) {
    const ui64 portionId = info.GetPortionId();
    if (!SelectedPortionIds.contains(portionId)) {
        return;
    }
    // A selected write committed, or an aborted rewrite/level move returned the same immutable blobs.
    UncommittedPortionIds.erase(portionId);
    InFlightPortionIds.erase(portionId);
    RetiredPortionIds.erase(portionId);
    RemoveFromActiveQueue(portionId);
    QueueSelectedPortion(info);
}

void TMoveDataActualizer::DoRemovePortion(const ui64 portionId) {
    if (!SelectedPortionIds.contains(portionId)) {
        return;
    }
    // Only metadata-confirmed target portions can hold the cleanup gate.
    RetiredPortionIds.emplace(portionId);
    InFlightPortionIds.erase(portionId);
    UncommittedPortionIds.erase(portionId);
    RemoveFromActiveQueue(portionId);
}

void TMoveDataActualizer::DoExtractTasks(
    TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext&) {
    if (!NYDBTest::TControllers::GetColumnShardController()->IsBackgroundEnabled(NYDBTest::ICSController::EBackground::MoveData)) {
        return;
    }
    THashSet<ui64> submitted;
    for (auto& [address, portions] : PortionsToMove) {
        if (!tasksContext.IsRWAddressAvailable(address)) {
            continue;
        }
        bool limitExceeded = false;
        for (auto& portionId : portions) {
            auto portion = externalContext.GetPortionVerified(portionId);
            auto portionSchema = portion->GetSchema(VersionedIndex);
            const TString tierName = portion->GetTierNameDef(IStoragesManager::DefaultStorageId);
            TPortionEvictionFeatures features(portionSchema, portionSchema, tierName);
            features.SetTargetTierName(tierName);
            features.SetForcedMove();

            switch (tasksContext.AddPortion(portion, std::move(features), TDuration::Zero())) {
                case TTieringProcessContext::EAddPortionResult::TASK_LIMIT_EXCEEDED:
                    limitExceeded = true;
                    break;
                case TTieringProcessContext::EAddPortionResult::PORTION_LOCKED:
                    break;
                case TTieringProcessContext::EAddPortionResult::SUCCESS:
                    submitted.emplace(portionId);
                    break;
            }
            if (limitExceeded) {
                break;
            }
        }
        if (limitExceeded) {
            break;
        }
    }
    for (auto portionId : submitted) {
        RemoveFromActiveQueue(portionId);
        InFlightPortionIds.emplace(portionId);
    }
}

TMoveDataQueueSizes TMoveDataActualizer::GetMoveDataQueueSizes(
    const THashMap<ui64, TPortionInfo::TPtr>& portions, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted) {
    for (auto it = RetiredPortionIds.begin(); it != RetiredPortionIds.end();) {
        if (portions.contains(*it) || uncommitted.contains(*it)) {
            ++it;
        } else {
            SelectedPortionIds.erase(*it);
            RetiredPortionIds.erase(it++);
        }
    }
    return TMoveDataQueueSizes{ .ConfirmedToMove = PortionAddress.size(), .InFlight = InFlightPortionIds.size(),
        .Uncommitted = UncommittedPortionIds.size(),
        .Retired = RetiredPortionIds.size() };
}

}   // namespace NKikimr::NOlap::NActualizer
