#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <memory_controller.h>
#include <memory_controller_config.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/tablet_memory_host.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tablet_flat/shared_cache_counters.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <util/generic/scope.h>

#ifdef _linux_
#include <sys/resource.h>
#endif

namespace NKikimr::NMemory {

using namespace Tests;
using namespace NSharedCache;

namespace {

void UpsertRows(TServer::TPtr server, TActorId sender, ui32 keyFrom = 0, ui32 keyTo = 2000) {
    TString query = "UPSERT INTO `/Root/table-1` (key, value) VALUES ";
    for (auto key : xrange(keyFrom, keyTo)) {
        if (key != keyFrom)
            query += ", ";
        query += "(" + ToString(key) + ", " + ToString(key) + ") ";
    }
    ExecSQL(server, sender, query);
}

// Bare-runtime tests need a provider they can drive without a whole server
struct TFixedProcessMemoryInfoProvider : public IProcessMemoryInfoProvider {
    TProcessMemoryInfo Get() const override {
        return ProcessMemoryInfo;
    }

    TProcessMemoryInfo ProcessMemoryInfo{0_MB, 0_MB, {}, {}, {}, {}};
};

// A memory controller on a bare runtime, driven through the provider and edge actors
struct TControllerFixture {
    TTestBasicRuntime Runtime;
    TIntrusivePtr<TFixedProcessMemoryInfoProvider> Provider = MakeIntrusive<TFixedProcessMemoryInfoProvider>();
    TIntrusivePtr<::NMonitoring::TDynamicCounters> CountersRoot = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TActorId MemoryController;

    explicit TControllerFixture(const NKikimrConfig::TMemoryControllerConfig& config, bool tabletMemoryHost = false) {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Runtime.GetAppData().FeatureFlags.SetEnableTabletMemoryHost(tabletMemoryHost);
        MemoryController = Runtime.Register(CreateMemoryController(
            TDuration::Seconds(1), TIntrusivePtr<IProcessMemoryInfoProvider>(Provider), config, TResourceBrokerConfig{}, CountersRoot));
        Runtime.EnableScheduleForActor(MemoryController);
        NActors::TDispatchOptions bootstrapOptions;
        bootstrapOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        Runtime.DispatchEvents(bootstrapOptions);
        Counters = GetServiceCounters(CountersRoot, "utils")->GetSubgroup("component", "memory_controller");
    }

    i64 Counter(const TString& name, bool derivative = false) const {
        return Counters->GetCounter(name, derivative)->Val();
    }

    TIntrusivePtr<IMemoryConsumer> Register(TActorId registrant, EMemoryConsumerKind kind) {
        Runtime.Send(new IEventHandle(MemoryController, registrant, new TEvConsumerRegister(kind)));
        return Runtime.GrabEdgeEvent<TEvConsumerRegistered>(registrant)->Get()->Consumer;
    }

    TIntrusivePtr<TTabletMemorySlot> RegisterSlot(TTabletTypes::EType tabletType, ui64 tabletId) {
        return TTabletMemoryHost::Instance().RegisterConsumer(tabletType, tabletId);
    }

    // A wake-up callable that sends TEvMemoryZone to the edge actor through the runtime's actor system
    std::function<void()> WakeEdge(TActorId edge) {
        return [&sys = *Runtime.GetAnyNodeActorSystem(), edge] {
            sys.Send(new IEventHandle(edge, {}, new TEvMemoryZone()));
        };
    }

    void Tick() {
        Runtime.SimulateSleep(TDuration::Seconds(2));
    }
};

class TWithMemoryControllerServer : public TServer {
public:
    TWithMemoryControllerServer(const TServerSettings& settings)
        : TServer(settings, false)
    {
        PreInitialize();
        Initialize();
    }

    void PrintCounters() const {
        Cerr << "SharedCache:" << Endl;
        Cerr << "    ActiveBytes = " << SharedPageCacheCounters->ActiveBytes->Val() << Endl;
        Cerr << "    PassiveBytes = " << SharedPageCacheCounters->PassiveBytes->Val() << Endl;
        Cerr << "    ConfigLimitBytes = " << SharedPageCacheCounters->ConfigLimitBytes->Val() << Endl;
        Cerr << "    MemLimitBytes = " << SharedPageCacheCounters->MemLimitBytes->Val() << Endl;
    }

private:
    void PreInitialize() {
        ProcessMemoryInfoProvider = MakeIntrusive<TFixedProcessMemoryInfoProvider>();
        ProcessMemoryInfo = &ProcessMemoryInfoProvider->ProcessMemoryInfo;

        // copy-paste from TMemoryControllerInitializer::InitializeServices
        NMemory::TResourceBrokerConfig resourceBrokerSelfConfig;
        const auto& resourceBrokerConfig = Settings->AppConfig->GetResourceBrokerConfig();
        if (resourceBrokerConfig.HasResourceLimit() && resourceBrokerConfig.GetResourceLimit().HasMemory()) {
            resourceBrokerSelfConfig.LimitBytes = resourceBrokerConfig.GetResourceLimit().GetMemory();
        }
        for (const auto& queue : resourceBrokerConfig.GetQueues()) {
            if (queue.HasLimit() && queue.GetLimit().HasMemory()) {
                resourceBrokerSelfConfig.QueueLimits[queue.GetName()] = queue.GetLimit().GetMemory();
            }
        }
        Cerr << "ResourceBrokerSelfConfig: " << resourceBrokerSelfConfig.ToString() << Endl;

        for (ui32 nodeIndex = 0; nodeIndex < Runtime->GetNodeCount(); ++nodeIndex) {
            Runtime->AddLocalService(MakeMemoryControllerId(nodeIndex),
                TActorSetupCmd(
                    CreateMemoryController(TDuration::Seconds(1), (TIntrusivePtr<IProcessMemoryInfoProvider>)ProcessMemoryInfoProvider,
                        Settings->AppConfig->GetMemoryControllerConfig(), resourceBrokerSelfConfig,
                        Runtime->GetDynamicCounters()),
                    TMailboxType::ReadAsFilled,
                    0),
                nodeIndex);
        }

        SharedPageCacheCounters = MakeHolder<TSharedPageCacheCounters>(GetServiceCounters(Runtime->GetDynamicCounters(), "tablets")->GetSubgroup("type", "S_CACHE"));
        MemoryControllerCounters = GetServiceCounters(Runtime->GetDynamicCounters(), "utils")->GetSubgroup("component", "memory_controller");

        Runtime->SetLogPriority(NKikimrServices::MEMORY_CONTROLLER, NLog::PRI_TRACE);
        Runtime->SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NLog::PRI_TRACE);
        Runtime->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NLog::PRI_TRACE);
    }

private:
    TIntrusivePtr<TFixedProcessMemoryInfoProvider> ProcessMemoryInfoProvider;

public:
    THolder<TSharedPageCacheCounters> SharedPageCacheCounters;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> MemoryControllerCounters;
    TProcessMemoryInfo* ProcessMemoryInfo;
};

// A killable registrant for the death-detection test; edge actors never die
class TStubRegistrant : public TActorBootstrapped<TStubRegistrant> {
public:
    TStubRegistrant(TActorId controller, EMemoryConsumerKind kind, TIntrusivePtr<IMemoryConsumer>* out)
        : Controller(controller)
        , Kind(kind)
        , Out(out)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(Controller, new TEvConsumerRegister(Kind));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvConsumerRegistered, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    void Handle(TEvConsumerRegistered::TPtr& ev) {
        *Out = ev->Get()->Consumer;
    }

private:
    const TActorId Controller;
    const EMemoryConsumerKind Kind;
    TIntrusivePtr<IMemoryConsumer>* const Out;
};

}

Y_UNIT_TEST_SUITE(TMemoryController) {

Y_UNIT_TEST(Counters) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(200_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 100_MB);

    server->ProcessMemoryInfo->AnonRss = 44_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 44_MB);

    server->ProcessMemoryInfo->AllocatedMemory = 33_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 33_MB);

    server->ProcessMemoryInfo->CGroupLimit = 1000_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 1000_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 200_MB);

    server->ProcessMemoryInfo->CGroupLimit = 100_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 100_MB);
}

#ifdef _linux_
Y_UNIT_TEST(Counters_MemMapsUnavailable) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_GT(server->MemoryControllerCounters->GetCounter("Stats/MemMapsCount")->Val(), 0);

    rlimit saved;
    UNIT_ASSERT_VALUES_EQUAL(getrlimit(RLIMIT_NOFILE, &saved), 0);
    rlimit noFiles = saved;
    noFiles.rlim_cur = 0;
    UNIT_ASSERT_VALUES_EQUAL(setrlimit(RLIMIT_NOFILE, &noFiles), 0);
    Y_DEFER { setrlimit(RLIMIT_NOFILE, &saved); };

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemMapsCount")->Val(), 0);
}
#endif

Y_UNIT_TEST(Counters_HardLimit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(1000_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 1000_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 750_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 500_MB);

    server->ProcessMemoryInfo->CGroupLimit = 200_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 100_MB);
}

Y_UNIT_TEST(Counters_NoHardLimit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 2_GB); // default
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 1536_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 1_GB);

    server->ProcessMemoryInfo->CGroupLimit = 200_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 100_MB);

    server->ProcessMemoryInfo->CGroupLimit = {};
    server->ProcessMemoryInfo->MemTotal = 220_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AnonRss")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/CGroupLimit")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/MemTotal")->Val(), 220_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/AllocatedMemory")->Val(), 0_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/HardLimit")->Val(), 220_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/SoftLimit")->Val(), 165_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/TargetUtilization")->Val(), 110_MB);
}

Y_UNIT_TEST(Config_ConsumerLimits) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();

    memoryControllerConfig->SetSharedCacheMinPercent(20);
    memoryControllerConfig->SetSharedCacheMaxPercent(30);
    memoryControllerConfig->SetSharedCacheMinBytes(100_MB);
    memoryControllerConfig->SetSharedCacheMaxBytes(500_MB);

    memoryControllerConfig->SetMemTableMinPercent(10);
    memoryControllerConfig->SetMemTableMaxPercent(20);
    memoryControllerConfig->SetMemTableMinBytes(10_MB);
    memoryControllerConfig->SetMemTableMaxBytes(50_MB);

    memoryControllerConfig->SetQueryExecutionLimitPercent(15);
    memoryControllerConfig->SetQueryExecutionLimitBytes(30_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    server->ProcessMemoryInfo->CGroupLimit = 1000_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMin")->Val(), 200_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMax")->Val(), 300_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMin")->Val(), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMax")->Val(), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 30_MB);

    server->ProcessMemoryInfo->CGroupLimit = 400_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMin")->Val(), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMax")->Val(), 120_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMin")->Val(), 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMax")->Val(), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 30_MB);

    server->ProcessMemoryInfo->CGroupLimit = 100_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMin")->Val(), 30_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/LimitMax")->Val(), 30_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMin")->Val(), 10_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/LimitMax")->Val(), 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 15_MB);
}

Y_UNIT_TEST(SharedCache) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(200_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);
    auto [shards, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
    UpsertRows(server, sender);
    CompactTable(runtime, shards[0], tableId1);

    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 32_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(94_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 30_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(66_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 70_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(40_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 90_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(40_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 120_MB; // exceeds soft limit
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(28_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->MemLimitBytes->Val());

    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->ActiveBytes->Val(), static_cast<i64>(32_KB), static_cast<i64>(5_KB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->PassiveBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveBytes->Val(), server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/Consumption")->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 1000_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->MemLimitBytes->Val());

    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveBytes->Val(), 0);
    UNIT_ASSERT_GT(server->SharedPageCacheCounters->PassiveBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->PassiveBytes->Val(), server->MemoryControllerCounters->GetCounter("Consumer/SharedCache/Consumption")->Val());
}

Y_UNIT_TEST(SharedCache_ConfigLimit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(300_MB);
    serverSettings.AppConfig->MutableSharedCacheConfig()->SetMemoryLimit(100_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();

    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), 100_MB);

    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 100_MB);
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(141_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->ConfigLimitBytes->Val());

    server->ProcessMemoryInfo->AllocatedMemory = 150_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    server->PrintCounters();
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ConfigLimitBytes->Val(), 100_MB);
    UNIT_ASSERT_DOUBLES_EQUAL(server->SharedPageCacheCounters->MemLimitBytes->Val(), static_cast<i64>(60_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_VALUES_EQUAL(server->SharedPageCacheCounters->ActiveLimitBytes->Val(), server->SharedPageCacheCounters->MemLimitBytes->Val());
}

Y_UNIT_TEST(MemTable) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetHardLimitBytes(200_MB);
    memoryControllerConfig->SetMemTableMinPercent(0);
    memoryControllerConfig->SetMemTableMaxPercent(100);
    memoryControllerConfig->SetMemTableMinBytes(100_KB);
    memoryControllerConfig->SetMemTableMaxBytes(10_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);
    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");
    UpsertRows(server, sender);

    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_DOUBLES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Limit")->Val(), static_cast<i64>(8_MB), static_cast<i64>(1_MB));
    UNIT_ASSERT_GT(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Consumption")->Val(), static_cast<i64>(100_KB));

    server->ProcessMemoryInfo->AllocatedMemory = 1000_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Limit")->Val(), static_cast<i64>(100_KB));
    UNIT_ASSERT_LE(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Consumption")->Val(), static_cast<i64>(100_KB));
    UNIT_ASSERT_GT(server->MemoryControllerCounters->GetCounter("Consumer/MemTable/Consumption")->Val(), static_cast<i64>(1_KB));
}

Y_UNIT_TEST(ResourceBroker) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetQueryExecutionLimitPercent(15);

    auto resourceBrokerConfig = serverSettings.AppConfig->MutableResourceBrokerConfig();
    auto queue = resourceBrokerConfig->AddQueues();
    queue->SetName("queue_cs_scan_read");
    queue->MutableLimit()->SetMemory(13_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = 1000_MB;
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();
    auto senderSubscriber = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue)));
    auto config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/ActivitiesLimitBytes")->Val(), 300_MB);

    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), senderSubscriber, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue, /*subscribe=*/ true)));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(senderSubscriber);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 150_MB);

    server->ProcessMemoryInfo->CGroupLimit = 500_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue)));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 75_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 75_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/ActivitiesLimitBytes")->Val(), 150_MB);

    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(senderSubscriber);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 75_MB);

    // ensure that other settings are not affected:
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest("queue_cs_scan_read")));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetCpu(), 3);
    UNIT_ASSERT_VALUES_EQUAL(config->Get()->QueueConfig->GetLimit().GetMemory(), 13_MB);
}

Y_UNIT_TEST(ResourceBroker_ConfigLimit) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetQueryExecutionLimitPercent(15);

    auto resourceBrokerConfig = serverSettings.AppConfig->MutableResourceBrokerConfig();
    resourceBrokerConfig->MutableResourceLimit()->SetMemory(1000_MB);
    auto queue = resourceBrokerConfig->AddQueues();
    queue->SetName(NLocalDb::KqpResourceManagerQueue);
    queue->MutableLimit()->SetMemory(999_MB);
    queue = resourceBrokerConfig->AddQueues();
    queue->SetName("queue_cs_scan_read");
    queue->MutableLimit()->SetMemory(13_MB);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = 500_MB;
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue)));
    auto config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(config->QueueConfig->GetLimit().GetMemory(), 999_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 999_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/ActivitiesLimitBytes")->Val(), 1000_MB);

    server->ProcessMemoryInfo->CGroupLimit = 200_MB;
    runtime.SimulateSleep(TDuration::Seconds(2));
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(NLocalDb::KqpResourceManagerQueue)));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(config->QueueConfig->GetLimit().GetMemory(), 999_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Consumer/QueryExecution/Limit")->Val(), 999_MB);
    UNIT_ASSERT_VALUES_EQUAL(server->MemoryControllerCounters->GetCounter("Stats/ActivitiesLimitBytes")->Val(), 1000_MB);

    // ensure that other settings are not affected:
    runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest("queue_cs_scan_read")));
    config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(config->QueueConfig->GetLimit().GetCpu(), 3);
    UNIT_ASSERT_VALUES_EQUAL(config->QueueConfig->GetLimit().GetMemory(), 13_MB);
}

Y_UNIT_TEST(ResourceBroker_ConfigCS) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    const ui64 compactionMemoryLimitPercent = 36;
    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetCompactionLimitPercent(compactionMemoryLimitPercent);

    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();
    InitRoot(server, sender);

    ui64 currentHardMemoryLimit = 1000_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));

    auto checkMemoryLimit = [&](const TString& queueName, const double coeff) {
        runtime.Send(new IEventHandle(MakeResourceBrokerID(), sender, new TEvResourceBroker::TEvConfigRequest(queueName)));
        auto config = runtime.GrabEdgeEvent<TEvResourceBroker::TEvConfigResponse>(handle);
        UNIT_ASSERT_DOUBLES_EQUAL_C(
            static_cast<double>(config->QueueConfig->GetLimit().GetMemory()),
            static_cast<double>(currentHardMemoryLimit * coeff * compactionMemoryLimitPercent / 100),
            1_KB,
            queueName << " " << coeff);
    };

    checkMemoryLimit(NLocalDb::ColumnShardCompactionIndexationQueue, ColumnTablesCompactionIndexationQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionTtlQueue, ColumnTablesTtlQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionGeneralQueue, ColumnTablesGeneralQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionNormalizerQueue, ColumnTablesNormalizerQueueFraction);

    Cerr << "Check memory change" << Endl;
    currentHardMemoryLimit = 100_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));

    checkMemoryLimit(NLocalDb::ColumnShardCompactionIndexationQueue, ColumnTablesCompactionIndexationQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionTtlQueue, ColumnTablesTtlQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionGeneralQueue, ColumnTablesGeneralQueueFraction);
    checkMemoryLimit(NLocalDb::ColumnShardCompactionNormalizerQueue, ColumnTablesNormalizerQueueFraction);
}

Y_UNIT_TEST(GroupedMemoryLimiter_ConfigCS) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    const ui64 compactionMemoryLimitPercent = 36;
    const ui64 readExecutionMemoryLimitPercent = 20;
    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetCompactionLimitPercent(compactionMemoryLimitPercent);
    memoryControllerConfig->SetQueryExecutionLimitPercent(readExecutionMemoryLimitPercent);

    ui64 currentHardMemoryLimit = 1000_MB;
    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();

    auto counters = runtime.GetAppData().Counters;
    auto compactionCounters = counters->GetSubgroup("module_id", "grouped_memory_limiter")->GetSubgroup("limiter_name", "Comp_0")->GetSubgroup("stage", "general");
    auto scanCounters = counters->GetSubgroup("module_id", "grouped_memory_limiter")->GetSubgroup("limiter_name", "Scan_0")->GetSubgroup("stage", "general");
    auto dedupCounters = counters->GetSubgroup("module_id", "grouped_memory_limiter")->GetSubgroup("limiter_name", "Dedu_0")->GetSubgroup("stage", "general");

    InitRoot(server, sender);

    auto checkMemoryLimits = [&]() {
        using OlapLimits = NKikimr::NOlap::TGlobalLimits;
        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * OlapLimits::GroupedMemoryLimiterSoftLimitCoefficient *
                (1.0 - ColumnTablesDeduplicationGroupedMemoryFraction) * readExecutionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TScanMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(scanCounters->GetCounter("Value/Limit/Soft/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * (1.0 - ColumnTablesDeduplicationGroupedMemoryFraction) * readExecutionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TScanMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(scanCounters->GetCounter("Value/Limit/Hard/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * OlapLimits::GroupedMemoryLimiterSoftLimitCoefficient *
                (1.0 - ColumnTablesDeduplicationGroupedMemoryFraction) * readExecutionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TDeduplicationMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(dedupCounters->GetCounter("Value/Limit/Soft/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * (1.0 - ColumnTablesDeduplicationGroupedMemoryFraction) * readExecutionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TDeduplicationMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(dedupCounters->GetCounter("Value/Limit/Hard/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * OlapLimits::GroupedMemoryLimiterSoftLimitCoefficient * compactionMemoryLimitPercent / 100 *
                NOlap::NGroupedMemoryManager::TCompMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(compactionCounters->GetCounter("Value/Limit/Soft/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * compactionMemoryLimitPercent / 100.0 *
                NOlap::NGroupedMemoryManager::TCompMemoryLimiterPolicy::HardLimitMultiplier),
            static_cast<double>(compactionCounters->GetCounter("Value/Limit/Hard/Bytes")->Val()),
            1_KB);
    };

    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();

    // Check memory decrease
    currentHardMemoryLimit = 500_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();

    // Check memory increase
    currentHardMemoryLimit = 2000_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();
}

Y_UNIT_TEST(ColumnShardCaches_Config) {
    using namespace NResourceBroker;

    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root").SetUseRealThreads(false);

    const ui64 sharedCacheMaxPercent = 50;
    auto memoryControllerConfig = serverSettings.AppConfig->MutableMemoryControllerConfig();
    memoryControllerConfig->SetSharedCacheMaxPercent(sharedCacheMaxPercent);

    ui64 currentHardMemoryLimit = 1000_MB;
    auto server = MakeIntrusive<TWithMemoryControllerServer>(serverSettings);
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    auto& runtime = *server->GetRuntime();
    TAutoPtr<IEventHandle> handle;
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);
    auto counters = runtime.GetAppData().Counters;
    auto dataAccessorCache = counters->GetSubgroup("module_id", "general_cache")->GetSubgroup("cache_name", "portions_metadata")->GetSubgroup("signals_owner", "manager");
    auto columnDataCache = counters->GetSubgroup("module_id", "general_cache")->GetSubgroup("cache_name", "column_data")->GetSubgroup("signals_owner", "manager");
    auto blobCache = counters->GetSubgroup("type", "BLOB_CACHE");

    auto checkMemoryLimits = [&]() {
        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * ColumnTablesPortionsMetaDataCacheFraction * sharedCacheMaxPercent / 100.0 * ColumnTablesCachesPercentFromShared / 100.0),
            static_cast<double>(NKikimr::NOlap::NStorageOptimizer::IOptimizerPlanner::GetPortionsCacheLimit()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * ColumnTablesColumnTablesDataAccessorCacheFraction * sharedCacheMaxPercent / 100.0 * ColumnTablesCachesPercentFromShared / 100.0),
            static_cast<double>(dataAccessorCache->GetCounter("Value/Cache/SizeLimit/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * ColumnTablesColumnDataCacheFraction * sharedCacheMaxPercent / 100.0 * ColumnTablesCachesPercentFromShared / 100.0),
            static_cast<double>(columnDataCache->GetCounter("Value/Cache/SizeLimit/Bytes")->Val()),
            1_KB);

        UNIT_ASSERT_DOUBLES_EQUAL(
            static_cast<double>(currentHardMemoryLimit * ColumnTablesBlobCacheFraction * sharedCacheMaxPercent / 100.0 * ColumnTablesCachesPercentFromShared / 100.0),
            static_cast<double>(blobCache->GetCounter("MaxSizeBytes")->Val()),
            1_KB);
    };

    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();

    // Check memory decrease
    currentHardMemoryLimit = 500_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();

    // Check memory increase
    currentHardMemoryLimit = 2000_MB;
    server->ProcessMemoryInfo->CGroupLimit = currentHardMemoryLimit;
    runtime.SimulateSleep(TDuration::Seconds(2));
    checkMemoryLimits();
}

Y_UNIT_TEST(ConsumerReportDegradedCoefficient) {
    // Min != Max for SharedCache, so the searched coefficient is what turns its limit
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    config.SetSharedCacheMinBytes(20_MB);
    config.SetSharedCacheMaxBytes(60_MB);
    config.SetMemTableMinBytes(10_MB);
    config.SetMemTableMaxBytes(10_MB);
    config.SetQueryExecutionLimitBytes(50_MB);
    TControllerFixture fixture(config);

    const TActorId sender = fixture.Runtime.AllocateEdgeActor();
    auto sharedCacheConsumer = fixture.Register(sender, EMemoryConsumerKind::SharedCache);
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, sender, new TEvMemTableRegister(1)));
    auto memTableConsumer = fixture.Runtime.GrabEdgeEvent<TEvMemTableRegistered>(sender)->Get()->Consumer;
    sharedCacheConsumer->SetConsumption(30_MB);
    memTableConsumer->SetConsumption(7_MB);

    // Nothing else allocated: the whole elastic range fits, so the search saturates at 1 - 2^-20
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/Coefficient"), 999999046);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 62914520);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Runtime.GrabEdgeEvent<TEvConsumerLimit>(sender)->Get()->LimitBytes, 62914520);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reservation"), 31457240);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/MemTable/Limit"), 10_MB);

    // Foreign allocation leaves less than the minimums demand: the search bottoms out and the limit is the minimum
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 120_MB;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/Coefficient"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reservation"), 0);

    // Interior point: the 100 MB target utilization minus 53 MB of other memory and the 10 MB memtable leaves 37 MB
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 90_MB;
    fixture.Tick();
    UNIT_ASSERT_DOUBLES_EQUAL(fixture.Counter("Stats/Coefficient") / 1e9, (37.0 - 20.0) / 40.0, 1e-5);
    UNIT_ASSERT_DOUBLES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit") / double(1_MB), 37.0, 0.01);
    UNIT_ASSERT_DOUBLES_EQUAL(fixture.Counter("Consumer/SharedCache/Reservation") / double(1_MB), 7.0, 0.01);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/MemTable/Limit"), 10_MB);
}

Y_UNIT_TEST(ConsumerReportClamp) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config);
    auto consumer = fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::ColumnTablesBlobCache);

    // An invalid report that breaks both invariants is clamped where MC reads it
    consumer->SetReport({.Used = 100_MB, .Demand = 10_MB, .Reclaimable = 500_MB});
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesBlobCache/Consumption"), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesBlobCache/Demand"), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/ColumnTablesBlobCache/Reclaimable"), 100_MB);
}

Y_UNIT_TEST(SetConsumptionForwardsDegradedReport) {
    struct TRecorder : public IMemoryConsumer {
        TConsumerReport Last;

        void SetReport(TConsumerReport report) override {
            Last = report;
        }
    };

    auto recorder = MakeIntrusive<TRecorder>();
    recorder->SetConsumption(42);
    UNIT_ASSERT_VALUES_EQUAL(recorder->Last.Used, 42);
    UNIT_ASSERT_VALUES_EQUAL(recorder->Last.Demand, 42);
    UNIT_ASSERT_VALUES_EQUAL(recorder->Last.Reclaimable, 0);
}

Y_UNIT_TEST(ConsumerKindSumsRegistrants) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    // Min == Max pins the kind limit to 60 MB, so the shares are exact regardless of the coefficient
    config.SetSharedCacheMinBytes(60_MB);
    config.SetSharedCacheMaxBytes(60_MB);
    TControllerFixture fixture(config);

    const TActorId first = fixture.Runtime.AllocateEdgeActor();
    const TActorId second = fixture.Runtime.AllocateEdgeActor();
    auto firstConsumer = fixture.Register(first, EMemoryConsumerKind::SharedCache);
    auto secondConsumer = fixture.Register(second, EMemoryConsumerKind::SharedCache);
    UNIT_ASSERT(firstConsumer.Get() != secondConsumer.Get());

    firstConsumer->SetReport({.Used = 10_MB, .Demand = 30_MB, .Reclaimable = 5_MB});
    secondConsumer->SetConsumption(20_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 30_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Demand"), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reclaimable"), 5_MB);

    // Demand fits the 60 MB limit: each registrant gets its demand plus half of the 10 MB left over
    UNIT_ASSERT_VALUES_EQUAL(fixture.Runtime.GrabEdgeEvent<TEvConsumerLimit>(first)->Get()->LimitBytes, 35_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Runtime.GrabEdgeEvent<TEvConsumerLimit>(second)->Get()->LimitBytes, 25_MB);

    // The unregistered entry leaves the sum and the survivor gets the whole limit back
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, second, new TEvConsumerUnregister(EMemoryConsumerKind::SharedCache)));
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 10_MB);
    // Skip the queued pre-unregister limits; every tick keeps producing one, so the drain must be bounded
    ui64 lastLimit = 0;
    for (size_t attempt = 0; attempt < 5 && lastLimit != 60_MB; ++attempt) {
        lastLimit = fixture.Runtime.GrabEdgeEvent<TEvConsumerLimit>(first)->Get()->LimitBytes;
    }
    UNIT_ASSERT_VALUES_EQUAL(lastLimit, 60_MB);
}

Y_UNIT_TEST(DeadRegistrantLeavesTheSum) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config);

    TIntrusivePtr<IMemoryConsumer> doomedConsumer;
    const TActorId doomed = fixture.Runtime.Register(new TStubRegistrant(fixture.MemoryController, EMemoryConsumerKind::SharedCache, &doomedConsumer));
    auto survivorConsumer = fixture.Register(fixture.Runtime.AllocateEdgeActor(), EMemoryConsumerKind::SharedCache);
    fixture.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    UNIT_ASSERT(doomedConsumer);

    doomedConsumer->SetConsumption(25_MB);
    survivorConsumer->SetConsumption(5_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 30_MB);

    // The poisoned registrant stops existing; the next tracked limit send detects the death
    fixture.Runtime.Send(new IEventHandle(doomed, TActorId(), new TEvents::TEvPoison));
    fixture.Runtime.SimulateSleep(TDuration::Seconds(3));
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumerRegistrantDeaths", true), 1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 5_MB);
}

Y_UNIT_TEST(ConsumerUnregisterDropsAccounting) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config);

    const TActorId owner = fixture.Runtime.AllocateEdgeActor();
    auto consumer = fixture.Register(owner, EMemoryConsumerKind::SharedCache);
    consumer->SetReport({.Used = 30_MB, .Demand = 40_MB, .Reclaimable = 10_MB});
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 30_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Demand"), 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reclaimable"), 10_MB);

    // A stale unregister from an actor that no longer owns the kind changes nothing
    const TActorId stranger = fixture.Runtime.AllocateEdgeActor();
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, stranger, new TEvConsumerUnregister(EMemoryConsumerKind::SharedCache)));
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 30_MB);

    // The owner's unregister takes its bytes out of the accounting and zeroes the gauges of the removed kind
    fixture.Runtime.Send(new IEventHandle(fixture.MemoryController, owner, new TEvConsumerUnregister(EMemoryConsumerKind::SharedCache)));
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Demand"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Reclaimable"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Limit"), 0);

    // Writes through the unregistered object no longer reach MC
    consumer->SetConsumption(5_MB);
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/ConsumersConsumption"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/SharedCache/Consumption"), 0);
}

Y_UNIT_TEST(TabletHostSlotFeedsTheAggregate) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config, /* tabletMemoryHost */ true);
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 150_MB;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/OtherConsumption"), 150_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Consumption"), 0);

    auto slot = fixture.RegisterSlot(TTabletTypes::KeyValue, 1);
    slot->SetReport({.Used = 100_MB, .Demand = 100_MB, .Reclaimable = 0});
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Consumption"), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Demand"), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/OtherConsumption"), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("TabletMemory/KeyValue/Used"), 100_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("TabletMemory/KeyValue/Demand"), 100_MB);
    // Attributed, not budgeted: the aggregate carries no limit
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Limit"), 0);

    // The released slot takes its last report out of the sums by the next tick
    slot.Reset();
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Consumption"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("TabletMemory/KeyValue/Used"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/OtherConsumption"), 150_MB);
}

Y_UNIT_TEST(TabletHostOffLeavesTheAggregateAbsent) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config);
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 150_MB;

    // What a tablet gets with the flag off: the report goes nowhere and the zone stays Green
    auto detached = TTabletMemorySlot::Detached();
    detached->SetReport({.Used = 100_MB, .Demand = 100_MB, .Reclaimable = 0});
    UNIT_ASSERT(!detached->IsAttached());
    UNIT_ASSERT_VALUES_EQUAL(TTabletMemoryHost::Instance().GetTotal(TTabletTypes::KeyValue).Used, 0);
    // Even a linked slot is invisible to a controller that runs with the flag off
    auto slot = fixture.RegisterSlot(TTabletTypes::KeyValue, 1);
    slot->SetReport({.Used = 100_MB, .Demand = 100_MB, .Reclaimable = 0});
    fixture.Tick();
    UNIT_ASSERT(!fixture.Counters->FindCounter("Consumer/Tablets/Consumption"));
    UNIT_ASSERT(!fixture.Counters->FindCounter("TabletMemory/KeyValue/Used"));
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/OtherConsumption"), 150_MB);
    UNIT_ASSERT(detached->GetZone() == EMemoryZone::Green);
}

Y_UNIT_TEST(TabletHostZoneFollowsAllocatedMemory) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config, /* tabletMemoryHost */ true);
    const TActorId asking = fixture.Runtime.AllocateEdgeActor();
    const TActorId quiet = fixture.Runtime.AllocateEdgeActor();
    auto first = fixture.RegisterSlot(TTabletTypes::KeyValue, 1);
    auto second = fixture.RegisterSlot(TTabletTypes::DataShard, 2);

    // Under the 100 MB target utilization: Green
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 50_MB;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/TabletMemoryZone"), 0);
    UNIT_ASSERT(first->GetZone() == EMemoryZone::Green);

    // Above the 150 MB soft limit: Red reaches every slot, the wake-up only the slot that asked
    first->RequestWakeup(fixture.WakeEdge(asking));
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 180_MB;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/TabletMemoryZone"), 2);
    UNIT_ASSERT(first->GetZone() == EMemoryZone::Red);
    UNIT_ASSERT(second->GetZone() == EMemoryZone::Red);
    UNIT_ASSERT(fixture.Runtime.GrabEdgeEvent<TEvMemoryZone>(asking));
    UNIT_ASSERT(!fixture.Runtime.GrabEdgeEvent<TEvMemoryZone>(quiet, TDuration::MilliSeconds(100)));

    // Between the target and the soft limit: Yellow; the request was one-shot
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 120_MB;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/TabletMemoryZone"), 1);
    UNIT_ASSERT(first->GetZone() == EMemoryZone::Yellow);
    UNIT_ASSERT(!fixture.Runtime.GrabEdgeEvent<TEvMemoryZone>(asking, TDuration::MilliSeconds(100)));

    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 80_MB;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Stats/TabletMemoryZone"), 0);
    UNIT_ASSERT(first->GetZone() == EMemoryZone::Green);
    UNIT_ASSERT(second->GetZone() == EMemoryZone::Green);
}

Y_UNIT_TEST(TabletHostWakesOnlyTheAskingSlot) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config);
    auto& host = TTabletMemoryHost::Instance();
    host.SetNodeZone(EMemoryZone::Green);
    auto first = fixture.RegisterSlot(TTabletTypes::KeyValue, 1);
    auto second = fixture.RegisterSlot(TTabletTypes::KeyValue, 2);
    ui32 firstWakeups = 0;

    first->RequestWakeup([&firstWakeups] { ++firstWakeups; });
    host.SetNodeZone(EMemoryZone::Yellow);
    UNIT_ASSERT_VALUES_EQUAL(firstWakeups, 1);
    UNIT_ASSERT(first->GetZone() == EMemoryZone::Yellow);
    UNIT_ASSERT(second->GetZone() == EMemoryZone::Yellow);

    // The request is one-shot: the next change wakes nobody until it is asked for again
    host.SetNodeZone(EMemoryZone::Red);
    UNIT_ASSERT_VALUES_EQUAL(firstWakeups, 1);
    UNIT_ASSERT(first->GetZone() == EMemoryZone::Red);

    // The same zone again is no change: the request stays pending
    first->RequestWakeup([&firstWakeups] { ++firstWakeups; });
    host.SetNodeZone(EMemoryZone::Red);
    UNIT_ASSERT_VALUES_EQUAL(firstWakeups, 1);
    host.SetNodeZone(EMemoryZone::Green);
    UNIT_ASSERT_VALUES_EQUAL(firstWakeups, 2);
    UNIT_ASSERT(first->GetZone() == EMemoryZone::Green);

    // A released slot takes its pending request with it
    second->RequestWakeup([] { UNIT_FAIL("a released slot must not be woken"); });
    second.Reset();
    host.SetNodeZone(EMemoryZone::Yellow);
    host.SetNodeZone(EMemoryZone::Green);
}

Y_UNIT_TEST(TabletHostElasticPartsGetSharesByDemand) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(2000_MB);
    TControllerFixture fixture(config, /* tabletMemoryHost */ true);
    auto first = fixture.RegisterSlot(TTabletTypes::KeyValue, 1);
    auto second = fixture.RegisterSlot(TTabletTypes::KeyValue, 2);
    auto third = fixture.RegisterSlot(TTabletTypes::DataShard, 3);
    first->SetReport({.Used = 80_MB, .Demand = 100_MB, .Reclaimable = 30_MB});
    second->SetReport({.Used = 40_MB, .Demand = 60_MB, .Reclaimable = 10_MB});
    third->SetReport({.Used = 5_MB, .Demand = 5_MB, .Reclaimable = 0});
    UNIT_ASSERT(!first->GetShare());

    // The non-elastic parts are Used - Reclaimable, the elastic parts are Reclaimable with the demand above the state
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 0;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Consumption"), 85_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Limit"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/TabletsElastic/Consumption"), 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/TabletsElastic/Demand"), 80_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/TabletsElastic/LimitMin"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/TabletsElastic/LimitMax"), 80_MB);
    // Plenty of memory: the coefficient is one and the limit is Max, the shares follow the elastic demand
    UNIT_ASSERT_DOUBLES_EQUAL(fixture.Counter("Consumer/TabletsElastic/Limit"), static_cast<i64>(80_MB), static_cast<i64>(1_KB));
    UNIT_ASSERT_DOUBLES_EQUAL(static_cast<i64>(*first->GetShare()), static_cast<i64>(50_MB), static_cast<i64>(1_KB));
    UNIT_ASSERT_DOUBLES_EQUAL(static_cast<i64>(*second->GetShare()), static_cast<i64>(30_MB), static_cast<i64>(1_KB));
    UNIT_ASSERT(!third->GetShare());
    // The per-type sums keep the whole reports
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("TabletMemory/KeyValue/Used"), 120_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("TabletMemory/KeyValue/Demand"), 160_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("TabletMemory/KeyValue/Reclaimable"), 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("TabletMemory/DataShard/Used"), 5_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("TabletMemory/DataShard/Reclaimable"), 0);

    // Exactly the limit: the demand-proportional split is exact
    TTabletMemoryHost::Instance().ApplyElasticLimit(80_MB);
    UNIT_ASSERT_VALUES_EQUAL(*first->GetShare(), 50_MB);
    UNIT_ASSERT_VALUES_EQUAL(*second->GetShare(), 30_MB);

    // Squeezed: the coefficient goes to zero, the limit is Min and every share is zero
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 2000_MB;
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/TabletsElastic/Limit"), 0);
    UNIT_ASSERT_VALUES_EQUAL(*first->GetShare(), 0);
    UNIT_ASSERT_VALUES_EQUAL(*second->GetShare(), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Consumption"), 85_MB);
    UNIT_ASSERT(!third->GetShare());

    // A slot that keeps an elastic part above its share still counts in full: nothing is clamped to the share
    first->SetReport({.Used = 80_MB, .Demand = 100_MB, .Reclaimable = 60_MB});
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/TabletsElastic/Consumption"), 70_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Consumption"), 55_MB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("TabletMemory/KeyValue/Used"), 120_MB);

    // Released slots leave both sums by the next tick
    first.Reset();
    second.Reset();
    fixture.Tick();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/TabletsElastic/Consumption"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/TabletsElastic/LimitMax"), 0);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Counter("Consumer/Tablets/Consumption"), 5_MB);
}

Y_UNIT_TEST(TabletHostElasticShareWakesOnlyTheAskingSlot) {
    NKikimrConfig::TMemoryControllerConfig config;
    config.SetHardLimitBytes(200_MB);
    TControllerFixture fixture(config);
    auto& host = TTabletMemoryHost::Instance();
    host.SetNodeZone(EMemoryZone::Green);
    auto first = fixture.RegisterSlot(TTabletTypes::KeyValue, 1);
    auto second = fixture.RegisterSlot(TTabletTypes::KeyValue, 2);
    auto plain = fixture.RegisterSlot(TTabletTypes::KeyValue, 3);
    first->SetReport({.Used = 20_MB, .Demand = 20_MB, .Reclaimable = 10_MB});
    second->SetReport({.Used = 60_MB, .Demand = 60_MB, .Reclaimable = 30_MB});
    plain->SetReport({.Used = 20_MB, .Demand = 20_MB, .Reclaimable = 0});
    ui32 firstWakeups = 0;
    ui32 plainWakeups = 0;

    // With the flag off the controller creates no elastic consumer, the host still sums the parts
    fixture.Provider->ProcessMemoryInfo.AllocatedMemory = 50_MB;
    fixture.Tick();
    UNIT_ASSERT(!fixture.Counters->FindCounter("Consumer/TabletsElastic/Consumption"));
    UNIT_ASSERT(!fixture.Counters->FindCounter("Consumer/Tablets/Consumption"));
    UNIT_ASSERT_VALUES_EQUAL(host.GetElasticTotal().Used, 40_MB);
    UNIT_ASSERT_VALUES_EQUAL(host.GetTotal().Used, 60_MB);

    // A share change wakes only the slot with an elastic part that asked; a slot without one gets no share
    first->RequestWakeup([&firstWakeups] { ++firstWakeups; });
    plain->RequestWakeup([&plainWakeups] { ++plainWakeups; });
    host.ApplyElasticLimit(40_MB);
    UNIT_ASSERT_VALUES_EQUAL(*first->GetShare(), 10_MB);
    UNIT_ASSERT_VALUES_EQUAL(*second->GetShare(), 30_MB);
    UNIT_ASSERT(!plain->GetShare());
    UNIT_ASSERT_VALUES_EQUAL(firstWakeups, 1);
    UNIT_ASSERT_VALUES_EQUAL(plainWakeups, 0);

    // The same limit again changes no share: the request stays pending
    first->RequestWakeup([&firstWakeups] { ++firstWakeups; });
    host.ApplyElasticLimit(40_MB);
    UNIT_ASSERT_VALUES_EQUAL(firstWakeups, 1);

    // A larger limit splits by the same proportions, and the changed share fires the pending request
    host.ApplyElasticLimit(80_MB);
    UNIT_ASSERT_VALUES_EQUAL(*first->GetShare(), 20_MB);
    UNIT_ASSERT_VALUES_EQUAL(*second->GetShare(), 60_MB);
    UNIT_ASSERT_VALUES_EQUAL(firstWakeups, 2);
    UNIT_ASSERT_VALUES_EQUAL(plainWakeups, 0);

    // The zone still reaches every slot, with or without an elastic part
    host.SetNodeZone(EMemoryZone::Yellow);
    UNIT_ASSERT_VALUES_EQUAL(plainWakeups, 1);
    UNIT_ASSERT(first->GetZone() == EMemoryZone::Yellow);
    host.SetNodeZone(EMemoryZone::Green);
}
}

}
