#include "tablet_info_helper.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NTxUT;
using namespace NColumnShard;

namespace {

using NTestMoveData::MakeTabletInfo;

constexpr ui32 OldGroup = 2181038080;
constexpr ui32 NewGroup = 2181038081;
constexpr ui32 ThirdGroup = 2181038082;

TActorId BootTablet(TTestBasicRuntime& runtime, const TIntrusivePtr<TTabletStorageInfo>& info, const TActorId& launcher = {}) {
    auto setupInfo = MakeIntrusive<TTabletSetupInfo>(&CreateColumnShard, TMailboxType::Simple, ui32(0), TMailboxType::Simple, ui32(0));
    const TActorId actorId = runtime.Register(CreateTablet(launcher, info.Get(), setupInfo.Get(), 0), 0);
    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);
    // EvBoot only starts boot: the shard stays in StateInit until its normalizers finish.
    runtime.DispatchEvents({}, TDuration::Seconds(1));
    return actorId;
}

// Portion data lives on channels 2+; the executor's vacuum leg moves the log and local DB.
std::vector<TLogoBlobID> LivePortionBlobs(const NFake::TProxyDS& proxy, const ui64 tabletId) {
    std::vector<TLogoBlobID> result;
    for (const auto& [id, blob] : proxy.AllMyBlobs()) {
        if (id.TabletID() == tabletId && id.Channel() >= 2 && !blob.DoNotKeep) {
            result.push_back(id);
        }
    }
    return result;
}

void RunMoveDataToCompletion(const bool ttlBackgroundDisabled, const bool moveDataEnabled = true) {
    TTestBasicRuntime runtime;
    runtime.SetScheduledLimit(10'000);
    TIntrusivePtr<NFake::TProxyDS> oldGroupProxy = new NFake::TProxyDS(TGroupId::FromValue(OldGroup));
    TIntrusivePtr<NFake::TProxyDS> newGroupProxy = new NFake::TProxyDS(TGroupId::FromValue(NewGroup));
    TTester::Setup(runtime,
        { new NFake::TProxyDS(TGroupId::FromValue(0)), oldGroupProxy, newGroupProxy, new NFake::TProxyDS(TGroupId::FromValue(Max<ui32>())) });
    runtime.GetAppData().FeatureFlags.SetEnableColumnshardGroupDecommission(moveDataEnabled);
    auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
    // Without a real mediator the rewrite plan-step never ages, so set staleness to zero.
    controller->SetOverrideMaxReadStaleness(TDuration::Zero());
    if (ttlBackgroundDisabled) {
        controller->DisableBackground(NYDBTest::ICSController::EBackground::TTL);
    }

    const ui64 tabletId = TTestTxConfig::TxTablet0;
    const ui64 tableId = 1;

    const TActorId tabletActorId = BootTablet(runtime, MakeTabletInfo(tabletId, { { 0, OldGroup } }));
    TActorId sender = runtime.AllocateEdgeActor();

    TestTableDescription table;
    auto planStep = SetupSchema(runtime, sender, tableId, table);
    std::vector<ui64> writeIds;
    UNIT_ASSERT(WriteData(runtime, sender, tabletId, /*writeId=*/1, tableId, MakeTestBlob({ 0, 1000 }, table.Schema), table.Schema, &writeIds));
    planStep = ProposeCommit(runtime, sender, tabletId, /*txId=*/1, writeIds);
    PlanCommit(runtime, sender, tabletId, planStep, TSet<ui64>{ 1 });
    controller->WaitCompactions(TDuration::Seconds(10));

    const std::vector<TLogoBlobID> before = LivePortionBlobs(*oldGroupProxy, tabletId);
    UNIT_ASSERT_C(before.size(), "nothing was written into OldGroup - the test would pass vacuously");

    // Reassign past everything written so far: those portions stay behind in OldGroup.
    ui32 reassignedFrom = 0;
    for (const auto& id : before) {
        reassignedFrom = Max(reassignedFrom, id.Generation() + 1);
    }
    runtime.Send(new IEventHandle(tabletActorId, tabletActorId, new TKikimrEvents::TEvPoisonPill));
    BootTablet(runtime, MakeTabletInfo(tabletId, { { 0, OldGroup }, { reassignedFrom, NewGroup } }));
    UNIT_ASSERT_VALUES_EQUAL_C(LivePortionBlobs(*newGroupProxy, tabletId).size(), 0u, "no portion data may exist in the target group yet");

    runtime.SendToPipe(tabletId, sender, new TEvTablet::TEvMoveData(std::vector<ui32>{ OldGroup }), 0, GetPipeConfigWithRetries());

    // Drive the gate manually; at iteration 25 commit an extra write to advance minSnapshotForNewReads.
    TEvTablet::TEvMoveDataResponse::TPtr response;
    bool advanceDone = false;
    // readStep is updated when the advance write commits — planStep becomes too old after that.
    auto readStep = planStep;
    for (ui32 i = 0; i < 150 && !response; ++i) {
        Wakeup(runtime, sender, tabletId);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        if (!advanceDone && i == 25) {
            advanceDone = true;
            std::vector<ui64> advIds;
            UNIT_ASSERT(WriteData(runtime, sender, tabletId, 2, tableId, MakeTestBlob({ 1000, 1001 }, table.Schema), table.Schema, &advIds));
            readStep = ProposeCommit(runtime, sender, tabletId, 2, advIds);
            PlanCommit(runtime, sender, tabletId, readStep, TSet<ui64>{ 2 });
        }
        response = runtime.GrabEdgeEventIf<TEvTablet::TEvMoveDataResponse>(sender, [](const TEvTablet::TEvMoveDataResponse::TPtr&) {
            return true;
        }, TDuration::MilliSeconds(100));
    }
    UNIT_ASSERT_C(response, "no TEvMoveDataResponse: the move never drained OldGroup");
    UNIT_ASSERT_VALUES_EQUAL((int)response->Get()->Record.GetStatus(), (int)NKikimrTabletBase::TEvMoveDataResponse::Success);

    // Success must mean rewritten, not merely empty queues: only the move puts data in NewGroup.
    const size_t movedBlobs = LivePortionBlobs(*newGroupProxy, tabletId).size();
    if (moveDataEnabled) {
        UNIT_ASSERT_C(movedBlobs, "answered Success without rewriting any of the " << before.size() << " portion blobs out of the old group");
    } else {
        UNIT_ASSERT_VALUES_EQUAL_C(movedBlobs, 0u, "the disabled feature flag still rewrote portions");
    }
    UNIT_ASSERT_VALUES_EQUAL(ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(readStep.Val(), 1), table.Schema)->num_rows(), 1000);
}

ui32 NextGeneration(const std::vector<TLogoBlobID>& blobs) {
    ui32 next = 0;
    for (const auto& id : blobs) {
        next = Max(next, id.Generation() + 1);
    }
    return next;
}

// History [G0, G, G2]: moving G out and cutting its entry must leave the earlier G0 entry and its rows alone.
void RunMiddleEntryCut() {
    TTestBasicRuntime runtime;
    runtime.SetScheduledLimit(10'000);
    TIntrusivePtr<NFake::TProxyDS> g0Proxy = new NFake::TProxyDS(TGroupId::FromValue(OldGroup));
    TIntrusivePtr<NFake::TProxyDS> gProxy = new NFake::TProxyDS(TGroupId::FromValue(NewGroup));
    TIntrusivePtr<NFake::TProxyDS> g2Proxy = new NFake::TProxyDS(TGroupId::FromValue(ThirdGroup));
    TTester::Setup(runtime,
        { new NFake::TProxyDS(TGroupId::FromValue(0)), g0Proxy, gProxy, g2Proxy, new NFake::TProxyDS(TGroupId::FromValue(Max<ui32>())) });
    runtime.GetAppData().FeatureFlags.SetEnableColumnshardGroupDecommission(true);
    auto& csConfig = runtime.GetAppData().ColumnShardConfig;
    csConfig.SetCutHistoryMeasureOnly(false);
    csConfig.SetCutHistoryProofSource(NKikimrConfig::TColumnShardConfig::CUT_HISTORY_PROOF_BS_RANGE);
    auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
    controller->SetOverrideMaxReadStaleness(TDuration::Zero());
    // Compaction would rewrite the G0-era portions into a later group and empty the G0 entry for real.
    controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);

    const ui64 tabletId = TTestTxConfig::TxTablet0;
    const ui64 tableId = 1;
    TActorId sender = runtime.AllocateEdgeActor();
    const TActorId launcher = runtime.AllocateEdgeActor();
    ui64 txId = 0;
    auto commitRows = [&](const ui64 from, const ui64 to, TestTableDescription& table) {
        std::vector<ui64> writeIds;
        ++txId;
        UNIT_ASSERT(WriteData(runtime, sender, tabletId, txId, tableId, MakeTestBlob({ from, to }, table.Schema), table.Schema, &writeIds));
        const auto step = ProposeCommit(runtime, sender, tabletId, txId, writeIds);
        PlanCommit(runtime, sender, tabletId, step, TSet<ui64>{ txId });
        return step;
    };
    auto restart = [&](const TActorId& tablet, const TIntrusivePtr<TTabletStorageInfo>& info) {
        runtime.Send(new IEventHandle(tablet, tablet, new TKikimrEvents::TEvPoisonPill));
        return BootTablet(runtime, info, launcher);
    };

    TActorId tablet = BootTablet(runtime, MakeTabletInfo(tabletId, { { 0, OldGroup } }), launcher);
    TestTableDescription table;
    Y_UNUSED(SetupSchema(runtime, sender, tableId, table));
    commitRows(0, 1000, table);
    const std::vector<TLogoBlobID> g0Blobs = LivePortionBlobs(*g0Proxy, tabletId);
    UNIT_ASSERT_C(g0Blobs.size(), "nothing was written into G0 - the test would pass vacuously");
    const ui32 gFrom = NextGeneration(g0Blobs);

    tablet = restart(tablet, MakeTabletInfo(tabletId, { { 0, OldGroup }, { gFrom, NewGroup } }));
    auto readStep = commitRows(1000, 2000, table);
    const std::vector<TLogoBlobID> gBlobs = LivePortionBlobs(*gProxy, tabletId);
    UNIT_ASSERT_C(gBlobs.size(), "nothing was written into G - the middle entry would be empty from the start");
    const ui32 g2From = NextGeneration(gBlobs);
    const std::vector<std::pair<ui32, ui32>> history = { { 0, OldGroup }, { gFrom, NewGroup }, { g2From, ThirdGroup } };

    tablet = restart(tablet, MakeTabletInfo(tabletId, history));
    runtime.SendToPipe(tabletId, sender, new TEvTablet::TEvMoveData(std::vector<ui32>{ NewGroup }), 0, GetPipeConfigWithRetries());
    TEvTablet::TEvMoveDataResponse::TPtr response;
    // As in RunMoveDataToCompletion, the advance write only moves the plan step and is not counted.
    const ui64 expectedRows = 2000;
    for (ui32 i = 0; i < 150 && !response; ++i) {
        Wakeup(runtime, sender, tabletId);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        if (i == 25) {
            readStep = commitRows(2000, 2001, table);
        }
        response = runtime.GrabEdgeEventIf<TEvTablet::TEvMoveDataResponse>(sender, [](const TEvTablet::TEvMoveDataResponse::TPtr&) {
            return true;
        }, TDuration::MilliSeconds(100));
    }
    UNIT_ASSERT_C(response, "no TEvMoveDataResponse: the move never drained G");
    UNIT_ASSERT_VALUES_EQUAL((int)response->Get()->Record.GetStatus(), (int)NKikimrTabletBase::TEvMoveDataResponse::Success);
    UNIT_ASSERT_VALUES_EQUAL_C(LivePortionBlobs(*gProxy, tabletId).size(), 0u, "G must be drained");
    UNIT_ASSERT_VALUES_EQUAL_C(LivePortionBlobs(*g0Proxy, tabletId).size(), g0Blobs.size(), "the move must not touch G0");

    // Hive restarts the tablet after the move; that boot proves the ranges and asks Hive to cut.
    TVector<std::tuple<ui32, ui32, ui32>> cuts;
    tablet = restart(tablet, MakeTabletInfo(tabletId, history));
    for (ui32 i = 0; i < 100; ++i) {
        Wakeup(runtime, sender, tabletId);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        while (auto cut = runtime.GrabEdgeEventIf<TEvTablet::TEvCutTabletHistory>(launcher, [](const TEvTablet::TEvCutTabletHistory::TPtr&) {
            return true;
        }, TDuration::MilliSeconds(10))) {
            const auto& record = cut->Get()->Record;
            cuts.emplace_back(record.GetChannel(), record.GetFromGeneration(), record.GetGroupID());
        }
    }

    THashSet<ui32> channelsWithG0Data;
    for (const auto& id : g0Blobs) {
        channelsWithG0Data.insert(id.Channel());
    }
    ui32 middleCuts = 0;
    for (const auto& [channel, fromGen, group] : cuts) {
        if (channel < 2) {
            continue;
        }
        if (group == NewGroup) {
            UNIT_ASSERT_VALUES_EQUAL(fromGen, gFrom);
            ++middleCuts;
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(group, OldGroup, "only G0 and G entries have successors");
            UNIT_ASSERT_C(!channelsWithG0Data.contains(channel), "a G0 entry was cut on channel " << channel << " that still holds G0 data");
        }
    }
    UNIT_ASSERT_C(middleCuts, "no G entry was cut after the move");

    // Boot the way Hive would after the cuts, then read every row: G0 rows still resolve to G0.
    auto cutInfo = MakeTabletInfo(tabletId, history);
    for (const auto& [channel, fromGen, group] : cuts) {
        auto& entries = cutInfo->Channels[channel].History;
        EraseIf(entries, [&](const TTabletChannelInfo::THistoryEntry& entry) {
            return entry.FromGeneration == fromGen && entry.GroupID == group;
        });
    }
    restart(tablet, cutInfo);
    UNIT_ASSERT_VALUES_EQUAL(ReadAllAsBatch(runtime, tableId, NOlap::TSnapshot(readStep.Val(), 1), table.Schema)->num_rows(), expectedRows);
    UNIT_ASSERT_VALUES_EQUAL_C(LivePortionBlobs(*g0Proxy, tabletId).size(), g0Blobs.size(), "the cut must not collect G0 data");
}

}   // namespace

// Whole chain: TEvMoveData -> selection -> accessor metadata -> rewrite -> response.
Y_UNIT_TEST_SUITE(TColumnShardMoveDataE2E) {
    Y_UNIT_TEST(MoveDataRewritesPortionsAndAnswersHive) {
        RunMoveDataToCompletion(/*ttlBackgroundDisabled=*/false);
    }

    // Rewrites come from the loop TTL uses: TTL off must not stop the move.
    Y_UNIT_TEST(MoveDataCompletesWithTtlDisabled) {
        RunMoveDataToCompletion(/*ttlBackgroundDisabled=*/true);
    }

    // Flag off: TEvMoveData goes straight to the executor, which leaves the portions alone.
    Y_UNIT_TEST(MoveDataDisabledLeavesPortionsInPlace) {
        RunMoveDataToCompletion(/*ttlBackgroundDisabled=*/false, /*moveDataEnabled=*/false);
    }

    // Decommission of a middle group: G is moved and cut, the earlier G0 entry and its rows survive.
    Y_UNIT_TEST(MiddleGroupDecommissionKeepsEarlierGroup) {
        RunMiddleEntryCut();
    }
}

}   // namespace NKikimr
