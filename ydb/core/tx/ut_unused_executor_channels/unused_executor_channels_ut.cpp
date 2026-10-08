#include <ydb/core/statistics/aggregator/aggregator.h>
#include <ydb/core/sys_view/processor/processor.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTest {
namespace {

using TCreateTablet = IActor* (*)(const TActorId&, TTabletStorageInfo*);

void CheckUnusedChannels(TTabletTypes::EType type, TCreateTablet createTablet) {
    TTestBasicRuntime runtime;
    TAppPrepare app;
    app.FeatureFlags.SetEnableCutHistory(true);
    app.AddDomain(TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
        "Root", 0, MakeTabletID(false, 1), 50,
        TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
        TVector<ui64>{TDomainsInfo::MakeTxMediatorIDFixed(1)},
        TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)}).Release());
    SetupTabletServices(runtime, &app, true);

    const ui64 tabletId = MakeTabletID(false, 1);
    TIntrusivePtr<TTabletStorageInfo> info = CreateTestTabletInfo(tabletId, type);
    // These channels have obsolete assignments but have never stored blobs.
    for (ui32 channel : {2, 3, 4}) {
        info->Channels[channel].History.emplace_back(1, 0);
    }

    THashSet<ui32> collectedChannels;
    auto observer = runtime.AddObserver([&](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage) {
            const auto* gc = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
            if (gc->TabletId == tabletId && gc->Channel >= 2 && !gc->Hard) {
                collectedChannels.insert(gc->Channel);
            }
        }
    });
    CreateTestBootstrapper(runtime, info.Get(), createTablet);
    TDispatchOptions options;
    options.CustomFinalCondition = [&] { return collectedChannels.size() == 3; };
    options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
    runtime.DispatchEvents(options, TDuration::Seconds(30));
    for (ui32 channel : {2, 3, 4}) {
        UNIT_ASSERT_C(collectedChannels.contains(channel), "Missing GC for channel " << channel);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TUnusedSystemTabletChannels) {
    Y_UNIT_TEST(Mediator) {
        CheckUnusedChannels(TTabletTypes::Mediator, CreateTxMediator);
    }

    Y_UNIT_TEST(SchemeShard) {
        CheckUnusedChannels(TTabletTypes::SchemeShard, CreateFlatTxSchemeShard);
    }

    Y_UNIT_TEST(SysViewProcessor) {
        CheckUnusedChannels(TTabletTypes::SysViewProcessor, NSysView::CreateSysViewProcessor);
    }

    Y_UNIT_TEST(StatisticsAggregator) {
        CheckUnusedChannels(TTabletTypes::StatisticsAggregator, NStat::CreateStatisticsAggregator);
    }
}

} // namespace NKikimr::NTest
