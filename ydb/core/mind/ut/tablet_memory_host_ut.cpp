#include <ydb/core/mind/tablet_memory_host.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NKikimr::NMemory {

namespace {

TActorId MakeExecutor(ui32 id) {
    return TActorId(1, 0, id, 0);
}

TTabletMemoryHost::TReportUpdate Report(ui64 used, ui64 demand, ui64 reclaimable) {
    return {.Used = used, .Demand = demand, .Reclaimable = reclaimable};
}

}

Y_UNIT_TEST_SUITE(TTabletMemoryHost) {

Y_UNIT_TEST(SplitsTheReportIntoStateAndElasticPart) {
    TTabletMemoryHost host;
    UNIT_ASSERT(host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 160, 40)));

    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 60u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 40u);
    // The elastic demand is what the tablet would hold beyond its state
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, 100u);

    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Used, 60u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetReport().Reclaimable, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Used, 40u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Demand, 100u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticReport().Reclaimable, 40u);
}

Y_UNIT_TEST(ClampsAReportThatBreaksItsInvariants) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 10, 500));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 100u);
}

Y_UNIT_TEST(AnOmittedFieldKeepsItsLastValue) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 160, 40));

    // One metrics tick carried only the used bytes
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, {.Used = 120});
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 80u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 40u);
}

Y_UNIT_TEST(ALeavingTabletTakesItsReportOutOfTheSums) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 40));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::KeyValue, Report(30, 30, 0));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 90u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 40u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 2u);

    UNIT_ASSERT(host.Forget({1, 0}));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 30u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 1u);

    UNIT_ASSERT(!host.Forget({1, 0}));
    UNIT_ASSERT(host.Forget({2, 0}));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 0u);
}

Y_UNIT_TEST(FollowersAndLeadersKeepSeparateSlots) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 0));
    host.SetReport({1, 1}, MakeExecutor(2), TTabletTypes::DataShard, Report(50, 50, 0));
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 150u);
}

Y_UNIT_TEST(ElasticLimitIsSplitByReclaimable) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 30));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(100, 100, 10));
    // A tablet with no elastic part is not in the split
    host.SetReport({3, 0}, MakeExecutor(3), TTabletTypes::DataShard, Report(100, 100, 0));

    auto shares = host.ApplyElasticLimit(40);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 2u);
    TMap<TActorId, ui64> byExecutor;
    for (const auto& share : shares) {
        byExecutor[share.Executor] = share.Bytes;
    }
    UNIT_ASSERT_VALUES_EQUAL(byExecutor[MakeExecutor(1)], 30u);
    UNIT_ASSERT_VALUES_EQUAL(byExecutor[MakeExecutor(2)], 10u);

    // Only the tablets whose share changed are reported again
    UNIT_ASSERT(host.ApplyElasticLimit(40).empty());
    UNIT_ASSERT_VALUES_EQUAL(host.ApplyElasticLimit(20).size(), 2u);

    // Nothing elastic left: the shares that were handed out are taken back
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, {.Reclaimable = 0});
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, {.Reclaimable = 0});
    shares = host.ApplyElasticLimit(20);
    UNIT_ASSERT_VALUES_EQUAL(shares.size(), 2u);
    for (const auto& share : shares) {
        UNIT_ASSERT_VALUES_EQUAL(share.Bytes, 0u);
    }
}

Y_UNIT_TEST(PerTypeSensorsFollowTheSlots) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 120, 20));
    host.SetReport({2, 0}, MakeExecutor(2), TTabletTypes::DataShard, Report(200, 200, 0));
    host.SetReport({3, 0}, MakeExecutor(3), TTabletTypes::KeyValue, Report(10, 10, 0));

    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    host.UpdateCounters(counters);
    const TString dataShard = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(TTabletTypes::DataShard) << "/";
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Used")->Val(), 300);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Demand")->Val(), 320);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Reclaimable")->Val(), 20);

    host.Forget({1, 0});
    host.UpdateCounters(counters);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Used")->Val(), 200);
    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(dataShard + "Reclaimable")->Val(), 0);
}

Y_UNIT_TEST(ClearDropsEverything) {
    TTabletMemoryHost host;
    host.SetReport({1, 0}, MakeExecutor(1), TTabletTypes::DataShard, Report(100, 100, 40));
    host.Clear();
    UNIT_ASSERT_VALUES_EQUAL(host.GetSlotsCount(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Total, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().Elastic, 0u);
    UNIT_ASSERT_VALUES_EQUAL(host.GetSums().ElasticDemand, 0u);
    UNIT_ASSERT(host.GetExecutors().empty());
}

}

}
