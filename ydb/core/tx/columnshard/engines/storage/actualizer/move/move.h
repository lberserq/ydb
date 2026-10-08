#pragma once

#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/queue_sizes.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NOlap {
class TWrittenPortionInfo;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NActualizer {

// Tracks only portions whose accessor metadata matched the driver's target-group filter.
class TMoveDataActualizer: public IActualizer {
    friend struct TMoveDataActualizerTestAccess;

private:
    const TVersionedIndex& VersionedIndex;
    THashSet<ui64> SelectedPortionIds;
    THashMap<TRWAddress, THashSet<ui64>> PortionsToMove;
    THashMap<ui64, TRWAddress> PortionAddress;
    THashSet<ui64> InFlightPortionIds;
    // Selected portions whose old blobs have not yet reached the GC queues.
    THashSet<ui64> RetiredPortionIds;
    // Selected writes waiting for commit or abort before they can be rewritten or cleaned up.
    THashSet<ui64> UncommittedPortionIds;

    void RemoveFromActiveQueue(ui64 portionId);
    void QueueSelectedPortion(const TPortionInfo& info);

protected:
    void DoAddPortion(const TPortionInfo& info, const TAddExternalContext& context) override;
    void DoRemovePortion(ui64 portionId) override;
    void DoExtractTasks(
        TTieringProcessContext& tasksContext, const TExternalTasksContext& externalContext, TInternalTasksContext& internalContext) override;

public:
    // Called only after group filtering, with the portion's current lifecycle state from the granule.
    void AddPortionToMove(const TPortionInfo& info);

    // Drop selected retired ids absent from both granule maps; ids still present there await cleanup.
    TMoveDataQueueSizes GetMoveDataQueueSizes(
        const THashMap<ui64, TPortionInfo::TPtr>& portions, const THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>& uncommitted);

    explicit TMoveDataActualizer(const TVersionedIndex& versionedIndex)
        : VersionedIndex(versionedIndex)
    {
    }
};

}   // namespace NKikimr::NOlap::NActualizer
