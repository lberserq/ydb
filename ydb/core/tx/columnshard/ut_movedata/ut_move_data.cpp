#include "tablet_info_helper.h"

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/blob_manager.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/gc.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/columnshard_move_data.h>
#include <ydb/core/tx/columnshard/data_locks/locks/list.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/move.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/max/meta.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/ro_controller.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>

namespace NKikimr {

namespace NOlap::NActualizer {
struct TMoveDataActualizerTestAccess {
    static size_t GetRetiredPortionIdsCount(const TMoveDataActualizer& actualizer) {
        return actualizer.RetiredPortionIds.size();
    }
};
}   // namespace NOlap::NActualizer

using NTestMoveData::MakeTabletInfo;

static constexpr ui32 BlobSize = 1_KB;
static constexpr ui32 OldGroup = 100;
static constexpr ui32 NewGroup = 200;
static constexpr ui32 ReassignGen = 5;

static NOlap::TUnifiedBlobId MakeDsBlobId(ui32 dsGroup, ui64 tabletId, ui32 gen, ui32 step, ui32 channel) {
    TLogoBlobID logo(tabletId, gen, step, channel, BlobSize, 0);
    return NOlap::TUnifiedBlobId(dsGroup, logo);
}

// Every actualizer test lives under one path; portion ids alone tell the cases apart.
static NOlap::TInternalPathId TestPathId() {
    return NOlap::TInternalPathId::FromRawValue(1);
}

// Builds a TIndexInfo with a MAX index; inheritPortionStorage controls where the index blob lives.
static NOlap::TIndexInfo MakeMaxIndexInfo(const bool inheritPortionStorage) {
    static constexpr ui32 kPkColId = 1;
    static constexpr ui32 kMaxIndexId = 100;
    NKikimrSchemeOp::TColumnTableSchema proto;
    *proto.MutableColumns()->Add() = NArrow::NTest::TTestColumn("pk", NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(kPkColId);
    proto.AddKeyColumnNames("pk");
    proto.SetVersion(1);
    *proto.AddIndexes() = NOlap::NIndexes::TIndexMetaContainer(std::make_shared<NOlap::NIndexes::NMax::TIndexMeta>(kMaxIndexId, "pk_max",
                                                                   NOlap::IStoragesManager::DefaultStorageId, inheritPortionStorage, kPkColId))
                              .SerializeToProto();
    auto cache = std::make_shared<NOlap::TSchemaObjectsCache>();
    auto result = NOlap::TIndexInfo::BuildFromProto(1, proto, NOlap::TTestStoragesManager::GetInstance(), cache);
    AFL_VERIFY(result);
    return std::move(*result);
}

// Builds a compacted portion assigned to the given tier.
static NOlap::TPortionInfo::TPtr MakeTieredPortion(const ui64 portionId, const TString& tierName, const NOlap::TIndexInfo& indexInfo) {
    TString serialized = NArrow::SerializeBatchNoCompression(NOlap::NTest::MakePortionTestPKBatch(10, 19));
    NKikimrTxColumnShard::TIndexPortionMeta metaProto;
    metaProto.SetIsCompacted(true);
    metaProto.SetPrimaryKeyBorders(serialized);
    metaProto.SetTierName(tierName);
    metaProto.MutableRecordSnapshotMin()->SetPlanStep(1);
    metaProto.MutableRecordSnapshotMin()->SetTxId(1);
    metaProto.MutableRecordSnapshotMax()->SetPlanStep(1);
    metaProto.MutableRecordSnapshotMax()->SetTxId(1);
    metaProto.SetDeletionsCount(0);
    metaProto.SetCompactionLevel(0);
    metaProto.SetRecordsCount(10);
    metaProto.SetColumnRawBytes(100);
    metaProto.SetColumnBlobBytes(100);
    metaProto.SetIndexRawBytes(0);
    metaProto.SetIndexBlobBytes(0);
    metaProto.SetNumSlices(1);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetPlanStep(1);
    metaProto.MutableCompactedPortion()->MutableAppearanceSnapshot()->SetTxId(1);
    NOlap::TPortionMetaConstructor metaConstructor;
    NOlap::TFakeGroupSelector groupSelector;
    AFL_VERIFY(metaConstructor.LoadMetadata(metaProto, indexInfo, groupSelector));
    NOlap::TCompactedPortionInfoConstructor constructor(TestPathId(), portionId);
    constructor.SetSchemaVersion(indexInfo.GetVersion());
    constructor.SetAppearanceSnapshot(NOlap::TSnapshot(1, 1));
    constructor.MutableMeta() = metaConstructor;
    return constructor.Build();
}

// A compacted default-tier portion: admission accepts it without a schema lookup.
static NOlap::TPortionInfo::TPtr MakeDefaultTierPortion(const ui64 portionId) {
    return NOlap::NTest::MakeTestCompactedPortion(TestPathId(), portionId, 10, 19, 10, NOlap::TSnapshot(1, 1), std::nullopt);
}

// Owns the cache and the versioned index an actualizer refers to for the whole test.
struct TActualizerSchema {
    std::shared_ptr<NOlap::TSchemaObjectsCache> Cache = std::make_shared<NOlap::TSchemaObjectsCache>();
    NOlap::TVersionedIndex Index;

    explicit TActualizerSchema(NOlap::TIndexInfo&& indexInfo) {
        Index.AddIndex(NOlap::TSnapshot(1, 1), Cache->UpsertIndexInfo(std::move(indexInfo)));
    }

    const NOlap::TIndexInfo& GetIndexInfo() const {
        return Index.GetLastSchema()->GetIndexInfo();
    }
};

static std::vector<std::shared_ptr<NOlap::TDataAccessorsRequest>> BuildMetadataRequests(
    const THashMap<ui64, NOlap::TPortionInfo::TPtr>& candidates, const THashMap<ui64, NOlap::TPortionInfo::TPtr>& knownPortions,
    const NOlap::TVersionedIndex& index, const ui64 softLimit) {
    NColumnShard::TMoveDataMetadataScan scan;
    for (const auto& [_, portion] : candidates) {
        scan.AddCandidate(*portion, index);
    }
    std::vector<std::shared_ptr<NOlap::TDataAccessorsRequest>> requests;
    while (scan.GetPendingCount()) {
        auto request = scan.BuildRequest(index.GetLastSchema(), softLimit, [&](const auto& address) {
            const auto it = knownPortions.find(address.second);
            return it != knownPortions.end() ? it->second : nullptr;
        });
        // Completing classification releases the submitted candidate ids before the next batch.
        UNIT_ASSERT_VALUES_EQUAL(scan.TakePendingPortions().size(), request->GetSize());
        if (!request->IsEmpty()) {
            requests.emplace_back(std::move(request));
        }
    }
    return requests;
}

// A blob manager of one generation over the OldGroup -> NewGroup history, with everything BuildGCTask needs.
struct TBlobManagerFixture {
    TActorSystemStub ActorSystemStub;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    std::shared_ptr<NOlap::TBlobManager> Manager;
    std::shared_ptr<NOlap::NDataSharing::TStorageSharedBlobsManager> Shared;
    std::shared_ptr<NOlap::NBlobOperations::TStorageCounters> StorageCounters;
    std::shared_ptr<NOlap::NBlobOperations::TRemoveGCCounters> Counters;

    TBlobManagerFixture(const ui64 tabletId, const ui32 tabletGen) {
        ActorSystemStub.AppData.Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TabletInfo = MakeTabletInfo(tabletId, { { 0, OldGroup }, { ReassignGen, NewGroup } }, TBlobStorageGroupType::ErasureNone);
        Manager = std::make_shared<NOlap::TBlobManager>(TabletInfo, tabletGen, NOlap::TTabletId(tabletId));
        Shared = std::make_shared<NOlap::NDataSharing::TStorageSharedBlobsManager>(
            NOlap::NBlobOperations::TGlobal::DefaultStorageId, NOlap::TTabletId(tabletId));
        StorageCounters = std::make_shared<NOlap::NBlobOperations::TStorageCounters>(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
        Counters = StorageCounters->GetConsumerCounter(NOlap::NBlobOperations::EConsumer::GC)->GetRemoveGCCounters();
    }

    std::shared_ptr<NOlap::NBlobOperations::NBlobStorage::TGCTask> BuildGCTask() {
        return Manager->BuildGCTask(NOlap::NBlobOperations::TGlobal::DefaultStorageId, Manager, Shared, Counters);
    }
};

Y_UNIT_TEST_SUITE(TMoveDataTest) {
    // BlobsToDelete leg: the group comes straight off TUnifiedBlobId (keep leg: TestMoveDataKeepQueue).
    Y_UNIT_TEST(TestMoveDataDeleteQueue) {
        static constexpr ui64 TabletId = 42;
        TBlobManagerFixture f(TabletId, 3);
        UNIT_ASSERT_C(!f.Manager->HasBlobsForGroups({ OldGroup }), "empty queues must match nothing");

        f.Manager->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeDsBlobId(OldGroup, TabletId, 1, 1, 2));
        UNIT_ASSERT_C(f.Manager->HasBlobsForGroups({ OldGroup }), "blob in the old group must match");
        UNIT_ASSERT_C(!f.Manager->HasBlobsForGroups({ NewGroup }), "the group it was not written to must not match");
    }

    // A delete-only GC task drains the queue and sets no barrier, and the gate must still wait for its commit.
    Y_UNIT_TEST(TestMoveDataGateHeldWhileDeleteOnlyGCInFlight) {
        auto controllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TReadOnlyController>();
        static constexpr ui64 TabletId = 44;
        static constexpr ui32 TabletGen = 3;
        TBlobManagerFixture f(TabletId, TabletGen);

        // The first GC of an incarnation collects up to the current step; once it commits, the next task has no barrier to set.
        const NOlap::TGenStep barrier(TabletGen, 0);
        UNIT_ASSERT_C(f.BuildGCTask(), "the first GC must set a barrier");
        f.Manager->OnGCStartOnComplete(barrier);
        UNIT_ASSERT_C(!f.Manager->HasCollectedBeforeCurrentGeneration(), "a barrier that BlobStorage has not acknowledged proves nothing");
        f.Manager->OnGCFinishedOnComplete(barrier);
        UNIT_ASSERT_C(f.Manager->HasCollectedBeforeCurrentGeneration(), "the committed first round covers every earlier generation");

        f.Manager->DeleteBlobOnComplete(NOlap::TTabletId(TabletId), MakeDsBlobId(OldGroup, TabletId, 1, 1, 2));
        auto task = f.BuildGCTask();
        UNIT_ASSERT_C(task, "a queued delete must produce a GC task");
        UNIT_ASSERT_C(f.Manager->HasBlobsForGroups({ OldGroup }), "the gate must stay closed while a delete-only GC task is in flight");
        UNIT_ASSERT_C(
            !f.Manager->HasBlobsForGroups({ NewGroup }), "a task that touches only the old group must not hold a move out of the new one");

        f.Manager->OnGCFinishedOnComplete(std::nullopt);
        UNIT_ASSERT_C(!f.Manager->HasBlobsForGroups({ OldGroup }), "the gate must open once the task commits");
    }

    // Keep leg: the group is resolved through TabletInfo->GroupFor(channel, generation).
    Y_UNIT_TEST(TestMoveDataKeepQueue) {
        static constexpr ui64 TabletId = 45;

        // Generation 3 < ReassignGen: batches allocate blobs resolving into OldGroup.
        TBlobManagerFixture f(TabletId, 3);
        auto batch = f.Manager->StartBlobBatch();
        batch.AllocateNextBlobId(TString("payload"));
        f.Manager->SaveBlobBatchOnComplete(std::move(batch));

        UNIT_ASSERT_C(f.Manager->HasBlobsForGroups({ OldGroup }), "BlobsToKeep: blob in old group must match via GroupFor");
        UNIT_ASSERT_C(!f.Manager->HasBlobsForGroups({ NewGroup }), "BlobsToKeep: new group must not match");

        // Generation 7 >= ReassignGen: same channels now resolve into NewGroup.
        NOlap::TBlobManager mgrNew(f.TabletInfo, 7, NOlap::TTabletId(TabletId));
        auto batchNew = mgrNew.StartBlobBatch();
        batchNew.AllocateNextBlobId(TString("payload"));
        mgrNew.SaveBlobBatchOnComplete(std::move(batchNew));

        UNIT_ASSERT_C(mgrNew.HasBlobsForGroups({ NewGroup }), "BlobsToKeep: blob after reassign must match new group");
        UNIT_ASSERT_C(!mgrNew.HasBlobsForGroups({ OldGroup }), "BlobsToKeep: old group must not match after reassign");
    }

    // Borrowed leg: the group comes from the persisted DS:<group>:<id> form, not our history.
    Y_UNIT_TEST(TestMoveDataBorrowedBlobs) {
        static constexpr ui64 TabletId = 46;
        static constexpr ui64 ForeignTabletId = 99;

        NOlap::NDataSharing::TStorageSharedBlobsManager shared(NOlap::IStoragesManager::DefaultStorageId, NOlap::TTabletId(TabletId));
        UNIT_ASSERT(!shared.HasBlobsForGroups({ OldGroup }));

        const auto borrowed = MakeDsBlobId(OldGroup, ForeignTabletId, /*gen=*/1, /*step=*/1, /*channel=*/2);
        UNIT_ASSERT(shared.UpsertBorrowedBlobOnLoad(borrowed, NOlap::TTabletId(ForeignTabletId)));

        UNIT_ASSERT_C(shared.HasBlobsForGroups({ OldGroup }), "borrowed blob in old group must match via GetDsGroup");
        UNIT_ASSERT_C(!shared.HasBlobsForGroups({ NewGroup }), "unrelated group must not match");
    }

    // Exercised directly: a TPortionDataAccessor needs arrow-backed metadata to build.
    Y_UNIT_TEST(HasBlobInGroupsSelectsOnlyTargetGroups) {
        static constexpr ui64 TabletId = 46;
        static constexpr ui32 TargetGroup = 100;
        static constexpr ui32 OtherGroup = 200;
        static constexpr ui32 ThirdGroup = 300;
        static constexpr ui32 Gen = 3;
        static constexpr ui32 Step = 1;
        static constexpr ui32 Channel = 2;
        const THashSet<ui32> targets{ TargetGroup };

        const auto inTarget = MakeDsBlobId(TargetGroup, TabletId, Gen, Step, Channel);
        const auto outsideTarget = MakeDsBlobId(OtherGroup, TabletId, Gen, Step, Channel);
        const auto thirdParty = MakeDsBlobId(ThirdGroup, TabletId, Gen, Step, Channel);

        using TScan = NColumnShard::TMoveDataMetadataScan;
        UNIT_ASSERT_C(TScan::HasBlobInGroups({ inTarget }, targets), "a blob in a target group must select the portion");
        UNIT_ASSERT_C(!TScan::HasBlobInGroups({ outsideTarget }, targets), "a blob outside the target groups must not select it");
        UNIT_ASSERT_C(
            TScan::HasBlobInGroups({ outsideTarget, inTarget }, targets), "one blob in a target group is enough, even alongside others");
        UNIT_ASSERT_C(!TScan::HasBlobInGroups({ outsideTarget, thirdParty }, targets), "no blob in a target group means the portion stays put");
    }

    Y_UNIT_TEST(UnselectedPortionDoesNotEnterMoveQueues) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(schema.Index);
        const auto portion = MakeDefaultTierPortion(1);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        actualizer.AddPortion(portion, NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions));
        actualizer.RemovePortion(1);
        const auto queues = actualizer.GetMoveDataQueueSizes(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(queues.GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(queues.Retired, 0);
    }

    Y_UNIT_TEST(SelectedPortionReaddedAfterALevelMoveIsStillMoved) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(schema.Index);
        const auto portion = MakeDefaultTierPortion(1);
        const THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        actualizer.AddPortionToMove(*portion);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes(portions, {}).ConfirmedToMove, 1);

        actualizer.RemovePortion(1);
        actualizer.AddPortion(portion, NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions));
        const auto readded = actualizer.GetMoveDataQueueSizes(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(readded.ConfirmedToMove, 1);
        UNIT_ASSERT_VALUES_EQUAL(readded.Retired, 0);
    }

    Y_UNIT_TEST(RetiredSelectedPortionHoldsTheGateUntilCleanupErasesIt) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(schema.Index);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, MakeDefaultTierPortion(1) } };
        actualizer.AddPortionToMove(*portions.at(1));
        actualizer.RemovePortion(1);
        const auto retired = actualizer.GetMoveDataQueueSizes(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(retired.GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(retired.Retired, 1);

        portions.emplace(2, MakeDefaultTierPortion(2));
        actualizer.RemovePortion(2);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes(portions, {}).Retired, 1);
        portions.erase(1);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes(portions, {}).Retired, 0);
    }

    Y_UNIT_TEST(PortionSelectedAfterRetirementWaitsForCleanupOnly) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(schema.Index);
        auto portion = MakeDefaultTierPortion(1);
        portion->SetRemoveSnapshot(NOlap::TSnapshot(2, 1));
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions{ { 1, portion } };
        // Metadata can arrive after compaction retired the portion, or scan a portion already retired at start.
        actualizer.AddPortionToMove(*portion);
        const auto selected = actualizer.GetMoveDataQueueSizes(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(selected.GetTotal(), 0);
        UNIT_ASSERT_VALUES_EQUAL(selected.Retired, 1);
        portions.erase(1);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes(portions, {}).Retired, 0);
    }

    Y_UNIT_TEST(RetiredBookkeepingDropsCleanedPortions) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NOlap::NActualizer::TMoveDataActualizer actualizer(schema.Index);
        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
        for (ui64 id = 1; id <= 3; ++id) {
            portions.emplace(id, MakeDefaultTierPortion(id));
            actualizer.AddPortionToMove(*portions.at(id));
        }
        const auto first = portions.at(1);
        actualizer.RemovePortion(1);
        actualizer.RemovePortion(2);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes(portions, {}).Retired, 2);

        portions.erase(1);
        const auto reconciled = actualizer.GetMoveDataQueueSizes(portions, {});
        UNIT_ASSERT_VALUES_EQUAL(reconciled.Retired, 1);
        UNIT_ASSERT_VALUES_EQUAL(reconciled.ConfirmedToMove, 1);
        UNIT_ASSERT_VALUES_EQUAL(NOlap::NActualizer::TMoveDataActualizerTestAccess::GetRetiredPortionIdsCount(actualizer), 1);

        // An unfinished selected portion can return after an aborted rewrite or a level move.
        actualizer.AddPortion(portions.at(2), NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions));
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes(portions, {}).ConfirmedToMove, 2);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes(portions, {}).Retired, 0);
        // Physically cleaned portions no longer have any remembered session membership.
        actualizer.AddPortion(first, NOlap::NActualizer::TAddExternalContext(TInstant::Seconds(1000), portions));
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes(portions, {}).ConfirmedToMove, 2);
        actualizer.RemovePortion(2);
        portions.erase(2);
        UNIT_ASSERT_VALUES_EQUAL(actualizer.GetMoveDataQueueSizes(portions, {}).Retired, 0);
        UNIT_ASSERT_VALUES_EQUAL(NOlap::NActualizer::TMoveDataActualizerTestAccess::GetRetiredPortionIdsCount(actualizer), 0);
    }

    Y_UNIT_TEST(MetadataBatchingUsesAdmissionSchema) {
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());
        NKikimrSchemeOp::TColumnTableSchema proto;
        proto.SetVersion(2);
        proto.AddKeyColumnNames("pk");
        for (ui32 id = 0; id < 8; ++id) {
            const TString name = id ? TStringBuilder() << "value" << id : TString("pk");
            *proto.AddColumns() = NArrow::NTest::TTestColumn(name, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64)).CreateColumn(id);
        }
        auto indexInfo = NOlap::TIndexInfo::BuildFromProto(1, proto, NOlap::TTestStoragesManager::GetInstance(), schema.Cache);
        UNIT_ASSERT(indexInfo);
        schema.Index.AddIndex(NOlap::TSnapshot(2, 1), schema.Cache->UpsertIndexInfo(std::move(*indexInfo)));
        const auto latestSchema = schema.Index.GetLastSchema();
        const auto oldPortion = MakeDefaultTierPortion(1);
        const ui64 portionMemory = oldPortion->PredictAccessorsMemory(latestSchema);
        UNIT_ASSERT_GT(portionMemory, oldPortion->PredictAccessorsMemory(oldPortion->GetSchema(schema.Index)));

        // Old-only, mixed-version and new-only batches must use the same estimator as admission.
        for (const ui32 newSchemaEvery : { 0, 2, 1 }) {
            THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
            for (ui64 id = 1; id <= 7; ++id) {
                const ui64 version = newSchemaEvery && id % newSchemaEvery == 0 ? 2 : 1;
                const auto& indexInfo = schema.Index.GetSchemaVerified(version)->GetIndexInfo();
                portions.emplace(id, MakeTieredPortion(id, NOlap::IStoragesManager::DefaultStorageId, indexInfo));
            }
            const auto requests = BuildMetadataRequests(portions, portions, schema.Index, 2 * portionMemory);
            std::vector<ui32> sizes;
            THashSet<ui64> ids;
            for (const auto& request : requests) {
                UNIT_ASSERT_LE(request->PredictAccessorsMemory(latestSchema), 2 * portionMemory);
                sizes.emplace_back(request->GetSize());
                for (const ui64 id : request->GetPortionIds()) {
                    UNIT_ASSERT(ids.emplace(id).second);
                }
            }
            Sort(sizes.begin(), sizes.end(), std::greater<ui32>());
            UNIT_ASSERT_VALUES_EQUAL(sizes, (std::vector<ui32>{ 2, 2, 2, 1 }));
            UNIT_ASSERT_VALUES_EQUAL(ids.size(), portions.size());
        }
    }

    Y_UNIT_TEST(MoveDataMetadataRequestsBatching) {
        static constexpr ui64 PortionsCount = 7;
        TActualizerSchema schema(NOlap::NTest::MakePortionTestIndexInfo());

        THashMap<ui64, NOlap::TPortionInfo::TPtr> portions;
        for (ui64 portionId = 1; portionId <= PortionsCount; ++portionId) {
            portions.emplace(portionId, MakeDefaultTierPortion(portionId));
        }
        const ui64 portionMemory = portions.at(1)->PredictAccessorsMemory(portions.at(1)->GetSchema(schema.Index));
        UNIT_ASSERT_GT(portionMemory, 0);

        auto buildWithLimit = [&](const ui64 softLimit, const THashMap<ui64, NOlap::TPortionInfo::TPtr>& knownPortions) {
            return BuildMetadataRequests(portions, knownPortions, schema.Index, softLimit);
        };

        auto batchSizes = [](const std::vector<std::shared_ptr<NOlap::TDataAccessorsRequest>>& requests) {
            std::vector<ui32> result;
            for (auto&& request : requests) {
                result.emplace_back(request->GetSize());
            }
            Sort(result.begin(), result.end(), std::greater<ui32>());
            return result;
        };

        auto portionIds = [](const std::vector<std::shared_ptr<NOlap::TDataAccessorsRequest>>& requests) {
            THashSet<ui64> result;
            for (auto&& request : requests) {
                for (auto&& portionId : request->GetPortionIds()) {
                    result.emplace(portionId);
                }
            }
            return result;
        };

        {
            const auto requests = buildWithLimit(3 * portionMemory, portions);
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(batchSizes(requests), (std::vector<ui32>{ 3, 3, 1 }));
            UNIT_ASSERT_VALUES_EQUAL(portionIds(requests).size(), PortionsCount);
        }
        {
            const auto requests = buildWithLimit(PortionsCount * portionMemory + 1, portions);
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(requests.front()->GetSize(), PortionsCount);
        }
        {
            // The testing default of 0 makes every portion its own request.
            const auto requests = buildWithLimit(0, portions);
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), PortionsCount);
            UNIT_ASSERT_VALUES_EQUAL(batchSizes(requests), (std::vector<ui32>(PortionsCount, 1)));
        }
        {
            auto knownPortions = portions;
            knownPortions.erase(PortionsCount);
            const auto requests = buildWithLimit(3 * portionMemory, knownPortions);
            const auto ids = portionIds(requests);
            UNIT_ASSERT_VALUES_EQUAL(ids.size(), PortionsCount - 1);
            UNIT_ASSERT_C(!ids.contains(PortionsCount), "a portion the engine no longer knows must not be requested");
        }
    }

    Y_UNIT_TEST(AdmissionAdmitsTieredPortionWithDefaultStorageEntity) {
        for (const bool inheritStorage : { false, true }) {
            TActualizerSchema schema(MakeMaxIndexInfo(inheritStorage));
            NColumnShard::TMoveDataMetadataScan scan;
            scan.AddCandidate(*MakeTieredPortion(1, "tier1", schema.GetIndexInfo()), schema.Index);
            UNIT_ASSERT_VALUES_EQUAL(scan.GetPendingCount(), inheritStorage ? 0 : 1);
        }
    }

    // DoIsLocked ignores the category, so Actualization-vs-Compaction separation rests on LockCategoriesInteraction; assert both ways.
    Y_UNIT_TEST(ActualizationLockAndCompactionExcludeEachOther) {
        using namespace NOlap::NDataLocks;
        const auto portion = MakeDefaultTierPortion(1);
        const std::vector<NOlap::TPortionInfo::TConstPtr> portions{ portion };

        {
            TManager manager;
            auto guard = manager.RegisterLock<TListPortionsLock>("movedata::rewrite", portions, ELockCategory::Actualization);
            UNIT_ASSERT_C(manager.IsLocked(*portion, ELockCategory::Compaction),
                "a compaction planner must see the move's Actualization lock, or it can pick a portion being rewritten");
            UNIT_ASSERT_C(manager.IsLocked(*portion, ELockCategory::Actualization), "the move must see its own lock");
            UNIT_ASSERT_C(!manager.IsLocked(*portion, ELockCategory::Scan), "a reader must not be blocked by the move");
        }
        {
            TManager manager;
            auto guard = manager.RegisterLock<TListPortionsLock>("compaction::merge", portions, ELockCategory::Compaction);
            UNIT_ASSERT_C(manager.IsLocked(*portion, ELockCategory::Actualization),
                "the move must see a compaction lock, or DoExtractTasks hands out a portion compaction owns");
        }
        {
            TManager manager;
            const std::vector<NOlap::TPortionInfo::TConstPtr> other{ MakeDefaultTierPortion(2) };
            auto guard = manager.RegisterLock<TListPortionsLock>("movedata::other", other, ELockCategory::Actualization);
            UNIT_ASSERT_C(!manager.IsLocked(*portion, ELockCategory::Compaction), "a lock on another portion must not block this one");
        }
    }

    // Recovered barriers and queues only; the keep list is sized to hit the per-task GC limit.
    class TRecoveredGcDb: public NOlap::IBlobManagerDb {
    public:
        TGenStep Last;
        TGenStep Prepared;
        std::vector<NOlap::TUnifiedBlobId> Keeps;

        bool LoadGCBarrierPreparation(TGenStep& genStep) override {
            genStep = Prepared;
            return true;
        }

        void SaveGCBarrierPreparation(const TGenStep&) override {
        }

        bool LoadLastGcBarrier(TGenStep& genStep) override {
            genStep = Last;
            return true;
        }

        void SaveLastGcBarrier(const TGenStep&) override {
        }

        bool LoadLists(std::vector<NOlap::TUnifiedBlobId>& blobsToKeep, NOlap::TTabletsByBlob&, const NOlap::IBlobGroupSelector*,
            const NOlap::TTabletId) override {
            blobsToKeep = Keeps;
            return true;
        }

        void AddBlobToKeep(const NOlap::TUnifiedBlobId&) override {
        }

        void EraseBlobToKeep(const NOlap::TUnifiedBlobId&) override {
        }

        void AddBlobToDelete(const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void EraseBlobToDelete(const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        bool LoadTierLists(const TString&, NOlap::TTabletsByBlob&, std::deque<NOlap::TUnifiedBlobId>&, const NOlap::TTabletId) override {
            return true;
        }

        void AddTierBlobToDelete(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void RemoveTierBlobToDelete(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void AddTierDraftBlobId(const TString&, const NOlap::TUnifiedBlobId&) override {
        }

        void RemoveTierDraftBlobId(const TString&, const NOlap::TUnifiedBlobId&) override {
        }

        void AddBlobSharing(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void RemoveBlobSharing(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void AddBorrowedBlob(const TString&, const NOlap::TUnifiedBlobId&, const NOlap::TTabletId) override {
        }

        void RemoveBorrowedBlob(const TString&, const NOlap::TUnifiedBlobId&) override {
        }
    };

    // A barrier recovered from an older generation proves nothing about this one: the all-history broadcast must repeat until one of this generation went out.
    Y_UNIT_TEST(FirstGCBroadcastRepeatsUntilABarrierOfThisGenerationGoesOut) {
        auto controllerGuard = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TReadOnlyController>();
        static constexpr ui64 TabletId = 47;
        static constexpr ui32 TabletGen = 7;
        static constexpr ui32 DataChannel = 2;
        // One more than the per-task keep limit, so the first task cannot advance past the recovered barrier.
        static constexpr ui32 KeepCount = 500001;

        TBlobManagerFixture f(TabletId, TabletGen);
        // The repeated broadcast is part of the MoveData contract and stays behind its flag.
        f.ActorSystemStub.AppData.FeatureFlags.SetEnableColumnshardMoveData(true);
        TRecoveredGcDb db;
        db.Last = TGenStep(4, 0);
        db.Prepared = TGenStep(4, 1);
        for (ui32 i = 0; i < KeepCount; ++i) {
            db.Keeps.emplace_back(NewGroup, TLogoBlobID(TabletId, ReassignGen, 1 + i / 1000, DataChannel, BlobSize, i % 1000));
        }
        UNIT_ASSERT(f.Manager->LoadState(db, NOlap::TTabletId(TabletId)));
        const NOlap::NBlobOperations::NBlobStorage::TBlobAddress oldGroupAddress(OldGroup, DataChannel);

        auto first = f.BuildGCTask();
        UNIT_ASSERT_C(first, "the recovered barrier must produce a task");
        UNIT_ASSERT_C(first->GetListsByGroupId().contains(oldGroupAddress), "the first task broadcasts to every historical group");
        f.Manager->OnGCStartOnComplete(TGenStep(4, 1));
        f.Manager->OnGCFinishedOnComplete(TGenStep(4, 1));
        UNIT_ASSERT_C(!f.Manager->HasCollectedBeforeCurrentGeneration(), "a barrier from generation 4 must not count for generation 7");

        auto second = f.BuildGCTask();
        UNIT_ASSERT_C(second, "the remaining keep entry must produce a task");
        UNIT_ASSERT_C(second->GetListsByGroupId().contains(oldGroupAddress),
            "the barrier of this generation must reach the historical group, or an orphan written before the crash stays behind the gate");
        f.Manager->OnGCStartOnComplete(TGenStep(TabletGen, 0));
        f.Manager->OnGCFinishedOnComplete(TGenStep(TabletGen, 0));
        UNIT_ASSERT(f.Manager->HasCollectedBeforeCurrentGeneration());
    }

}   // Y_UNIT_TEST_SUITE

}   // namespace NKikimr
