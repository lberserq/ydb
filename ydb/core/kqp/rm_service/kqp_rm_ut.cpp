#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/tablet/resource_broker_impl.h>
#include <ydb/core/base/memory_controller_iface.h>

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/node_service/kqp_query_control_plane.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/size_literals.h>

#ifndef NDEBUG
const bool DETAILED_LOG = false;
#else
const bool DETAILED_LOG = true;
#endif

namespace NKikimr {
namespace NKqp {

using namespace NKikimrResourceBroker;
using namespace NResourceBroker;

namespace {

TTenantTestConfig MakeTenantTestConfig() {
    TTenantTestConfig cfg = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{{DOMAIN1_NAME, SCHEME_SHARD1_ID, {{TENANT1_1_NAME, TENANT1_2_NAME}}}}},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        true,
        // CreateConsole
        false,
        // Nodes
        {{
             // Node0
             {
                 // TenantPoolConfig
                 {
                     // Static slots {tenant, {cpu, memory, network}}
                     {{{DOMAIN1_NAME, {1, 1, 1}}}},
                     "node-type"
                 }
             },
             // Node1
             {
                 // TenantPoolConfig
                 {
                     // Static slots {tenant, {cpu, memory, network}}
                     {{{DOMAIN1_NAME, {1, 1, 1}}}},
                     "node-type"
                 }
             }
         }},
        // DataCenterCount
        1
    };
    return cfg;
}

TResourceBrokerConfig MakeResourceBrokerTestConfig() {
    TResourceBrokerConfig config;

    auto queue = config.AddQueues();
    queue->SetName("queue_default");
    queue->SetWeight(5);
    queue->MutableLimit()->AddResource(4);

    queue = config.AddQueues();
    queue->SetName("queue_kqp_resource_manager");
    queue->SetWeight(20);
    queue->MutableLimit()->AddResource(4);
    queue->MutableLimit()->AddResource(50'000);

    auto task = config.AddTasks();
    task->SetName("unknown");
    task->SetQueueName("queue_default");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::KqpResourceManagerTaskName);
    task->SetQueueName("queue_kqp_resource_manager");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    config.MutableResourceLimit()->AddResource(10);
    config.MutableResourceLimit()->AddResource(100'000);

    return config;
}

NKikimrConfig::TTableServiceConfig::TResourceManager MakeKqpResourceManagerConfig() {
    NKikimrConfig::TTableServiceConfig::TResourceManager config;

    config.SetComputeActorsCount(100);
    config.SetPublishStatisticsIntervalSec(0);
    config.SetQueryMemoryLimit(1000);

    auto* infoExchangerRetrySettings = config.MutableInfoExchangerSettings();
    auto* exchangerSettings = infoExchangerRetrySettings->MutableExchangerSettings();
    exchangerSettings->SetStartDelayMs(50);
    exchangerSettings->SetMaxDelayMs(50);

    return config;
}

NKikimrConfig::TTableServiceConfig::TResourceManager MakeKqpResourceManagerConfigWithMC() {
    auto config = MakeKqpResourceManagerConfig();
    config.SetEnableMemoryControllerBudget(true);
    config.SetMkqlHeavyProgramMemoryLimit(1); // floor=1 so test limits are not raised
    return config;
}

}

class KqpRm : public TTestBase {
public:
    void SetUp() override {
        Runtime = MakeHolder<TTenantTestRuntime>(MakeTenantTestConfig());

        NActors::NLog::EPriority priority = DETAILED_LOG ? NLog::PRI_DEBUG : NLog::PRI_ERROR;
        Runtime->SetLogPriority(NKikimrServices::RESOURCE_BROKER, priority);
        Runtime->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, priority);

        auto now = Now();
        Runtime->UpdateCurrentTime(now);

        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

        for (ui32 nodeIndex = 0; nodeIndex < Runtime->GetNodeCount(); ++nodeIndex) {
            auto resourceBrokerConfig = MakeResourceBrokerTestConfig();
            auto broker = CreateResourceBrokerActor(resourceBrokerConfig, Counters);
            auto resourceBrokerActorId = Runtime->Register(broker, nodeIndex);
            ResourceBrokers.push_back(resourceBrokerActorId);
        }
        WaitForBootstrap();
    }

    void TearDown() override {
        ResourceBrokers.clear();
        ResourceManagers.clear();
        Runtime.Reset();
    }

    void WaitForBootstrap() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(Runtime->DispatchEvents(options));
    }

    void CreateKqpResourceManager(
            const NKikimrConfig::TTableServiceConfig::TResourceManager& config, ui32 nodeInd = 0) {
        auto kqpCounters = MakeIntrusive<TKqpCounters>(Counters);
        auto resman = CreateKqpResourceManagerActor(config, kqpCounters, ResourceBrokers[nodeInd], nullptr, Runtime->GetNodeId(nodeInd));
        // RM creates children during its registration, we need to enable schedule for them
        auto prevObserver = Runtime->SetRegistrationObserverFunc([](TTestActorRuntimeBase& runtime, const TActorId& /*parentId*/, const TActorId& actorId) {
            runtime.EnableScheduleForActor(actorId, true);
        });
        ResourceManagers.push_back(Runtime->Register(resman, nodeInd));
        Runtime->RegisterService(MakeKqpResourceManagerServiceID(
            Runtime->GetNodeId(nodeInd)), ResourceManagers.back(), nodeInd);
        Runtime->SetRegistrationObserverFunc(prevObserver);
    }

    void StartRms(const TVector<NKikimrConfig::TTableServiceConfig::TResourceManager>& configs = {}) {
        for (ui32 nodeIndex = 0; nodeIndex < Runtime->GetNodeCount(); ++nodeIndex) {
            if (configs.empty()) {
                CreateKqpResourceManager(MakeKqpResourceManagerConfig(), nodeIndex);
            } else {
                CreateKqpResourceManager(configs[nodeIndex], nodeIndex);
            }
        }
        WaitForBootstrap();
    }

    void AssertResourceBrokerSensors(i64 cpu, i64 mem, i64 enqueued, std::optional<i64> finished, i64 infly) {
        auto q = Counters->GetSubgroup("queue", "queue_kqp_resource_manager");
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("EnqueuedTasks")->Val(), enqueued);
        if (finished) {
            UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("FinishedTasks")->Val(), *finished);
        }
        UNIT_ASSERT_VALUES_EQUAL(q->GetCounter("InFlyTasks")->Val(), infly);

        auto t = Counters->GetSubgroup("task", "kqp_query");
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("CPUConsumption")->Val(), cpu);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("MemoryConsumption")->Val(), mem);
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("EnqueuedTasks")->Val(), enqueued);
        if (finished) {
            UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("FinishedTasks")->Val(), *finished);
        }
        UNIT_ASSERT_VALUES_EQUAL(t->GetCounter("InFlyTasks")->Val(), infly);
    }

    TIntrusivePtr<NRm::TTxState> MakeTx(ui64 txId, std::shared_ptr<NRm::IKqpResourceManager> rm) {
        return MakeIntrusive<NRm::TTxState>(rm, txId, TInstant::Now(), "", (double)100, "", false);
    }

    TIntrusivePtr<NRm::TTxState> MakePoolTx(ui64 txId, std::shared_ptr<NRm::IKqpResourceManager> rm, double memoryPoolPercent) {
        return MakeIntrusive<NRm::TTxState>(rm, txId, TInstant::Now(), "pool", memoryPoolPercent, "db", false);
    }

    void AssertResourceManagerStats(
            std::shared_ptr<NRm::IKqpResourceManager> rm, ui64 scanQueryMemory, ui32 executionUnits) {
        Y_UNUSED(executionUnits);
        auto stats = rm->GetLocalResources();
        UNIT_ASSERT_VALUES_EQUAL(scanQueryMemory, stats.Memory);
        UNIT_ASSERT_VALUES_EQUAL(executionUnits, stats.ExecutionUnits);
    }

    void Disconnect(ui32 nodeIndexFrom, ui32 nodeIndexTo) {
        const TActorId proxy = Runtime->GetInterconnectProxy(nodeIndexFrom, nodeIndexTo);

        Runtime->Send(
            new IEventHandle(
                proxy,  TActorId(), new TEvInterconnect::TEvDisconnect(), 0, 0),
                nodeIndexFrom, true);

        //Wait for event TEvInterconnect::EvNodeDisconnected
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvInterconnect::EvNodeDisconnected);
        Runtime->DispatchEvents(options);
    }

    struct TCheckedResources {
        ui64 ScanQueryMemory;
        ui32 ExecutionUnits;

        bool operator==(const TCheckedResources& other) const {
            return ScanQueryMemory == other.ScanQueryMemory &&
                ExecutionUnits == other.ExecutionUnits;
        }
    };

    void CheckSnapshot(ui32 nodeIndToCheck, TVector<TCheckedResources> verificationData,
            std::shared_ptr<NRm::IKqpResourceManager> currentRm) {
        TVector<NKikimrKqp::TKqpNodeResources> snapshot;
        std::atomic<int> ready = 0;

        while(true) {
            currentRm->RequestClusterResourcesInfo(
                    [&](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
                snapshot = std::move(resources);
                ready = 1;
            });

            while (ready.load() != 1) {
                Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
            }

            if (snapshot.size() != verificationData.size()) {
                continue;
            }
            std::sort(snapshot.begin(), snapshot.end(), [](auto first, auto second) {
                return first.GetNodeId() < second.GetNodeId();
            });

            TVector<TCheckedResources> currentData;
            std::transform(snapshot.cbegin(), snapshot.cend(), std::back_inserter(currentData),
                   [](const NKikimrKqp::TKqpNodeResources& cur) {
                        return TCheckedResources{cur.GetMemory()[0].GetAvailable(), cur.GetExecutionUnits()};
            });

            if (verificationData[nodeIndToCheck] == currentData[nodeIndToCheck]) {
                for (ui32 i = 0; i < verificationData.size(); i++) {
                    if (i != nodeIndToCheck) {
                        UNIT_ASSERT_VALUES_EQUAL(verificationData[i].ScanQueryMemory, currentData[i].ScanQueryMemory);
                    }
                }
                break;
            }
        }
    }

    UNIT_TEST_SUITE(KqpRm);
        UNIT_TEST(SingleTask);
        UNIT_TEST(ManyTasks);
        UNIT_TEST(NotEnoughMemory);
        UNIT_TEST(NotEnoughExecutionUnits);
        UNIT_TEST(ResourceBrokerNotEnoughResources);
        UNIT_TEST(SingleSnapshotByExchanger);
        UNIT_TEST(Reduce);
        UNIT_TEST(ConcurrentTasks);
        UNIT_TEST(ConcurrentChannels);
        UNIT_TEST(MemoryAvailability);
        UNIT_TEST(PoolMemoryAvailability);
        UNIT_TEST(TaskQuotaManagerOptional);
        UNIT_TEST(SnapshotSharingByExchanger);
        UNIT_TEST(NodesMembershipByExchanger);
        UNIT_TEST(DisonnectNodes);
        UNIT_TEST(MemoryControllerBudget_Registers);
        UNIT_TEST(MemoryControllerBudget_FlagOff);
        UNIT_TEST(MemoryControllerBudget_DynamicLimit);
        UNIT_TEST(MemoryControllerBudget_RbConfigLogOnly);
        UNIT_TEST(MemoryControllerBudget_RbConfigIgnoredEvenWhenValuesAgree);
        UNIT_TEST(MemoryControllerBudget_StalenessRevert);
        UNIT_TEST(MemoryControllerBudget_RecoveryAfterRevert);
    UNIT_TEST_SUITE_END();

    void SingleTask();
    void ManyTasks();
    void NotEnoughMemory();
    void NotEnoughExecutionUnits();
    void ResourceBrokerNotEnoughResources();
    void Snapshot();
    void SingleSnapshotByExchanger();
    void Reduce();
    void ConcurrentTasks();
    void ConcurrentChannels();
    void MemoryAvailability();
    void PoolMemoryAvailability();
    void TaskQuotaManagerOptional();
    void SnapshotSharing();
    void SnapshotSharingByExchanger();
    void NodesMembership();
    void NodesMembershipByExchanger();
    void DisonnectNodes();
    void MemoryControllerBudget_Registers();
    void MemoryControllerBudget_FlagOff();
    void MemoryControllerBudget_DynamicLimit();
    void MemoryControllerBudget_RbConfigLogOnly();
    void MemoryControllerBudget_RbConfigIgnoredEvenWhenValuesAgree();
    void MemoryControllerBudget_StalenessRevert();
    void MemoryControllerBudget_RecoveryAfterRevert();

private:
    THolder<TTestBasicRuntime> Runtime;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TVector<TActorId> ResourceBrokers;
    TVector<TActorId> ResourceManagers;
};
UNIT_TEST_SUITE_REGISTRATION(KqpRm);


void KqpRm::SingleTask() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto stats = rm->GetLocalResources();
    UNIT_ASSERT_VALUES_EQUAL(1000, stats.Memory);

    NRm::TKqpResourcesRequest request{.ExecutionUnits = 1, .Memory = 100};

    {
        auto tx = MakeTx(1, rm);
        auto task = 2;

        bool allocated = rm->AllocateResources(*tx, task, request);
        UNIT_ASSERT(allocated);

        AssertResourceManagerStats(rm, 900, 99);
        AssertResourceBrokerSensors(0, 100, 0, 0, 1);

        rm->FreeResources(*tx, task, request);
        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, 0, 1);
    }

    AssertResourceBrokerSensors(0, 0, 0, 1, 0);
}

void KqpRm::ManyTasks() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request{.ExecutionUnits = 1, .Memory = 100};

    {
        auto tx = MakeTx(1, rm);
        for (ui32 i = 1; i < 10; ++i) {
            auto task = i;
            bool allocated = rm->AllocateResources(*tx, task, request);
            UNIT_ASSERT(allocated);

            AssertResourceManagerStats(rm, 1000 - 100 * i, 100 - i);
            AssertResourceBrokerSensors(0, 100 * i, 0, i - 1, 1);
        }
    }

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 9, 0);
}

void KqpRm::NotEnoughMemory() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 2;

    bool allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 10, .Memory = 10'000});
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpRm::NotEnoughExecutionUnits() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 2;

    bool allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 1000, .Memory = 100});
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, 1000, 100);
    AssertResourceBrokerSensors(0, 0, 0, 0, 0);
}

void KqpRm::ResourceBrokerNotEnoughResources() {
    auto config = MakeKqpResourceManagerConfig();
    config.SetQueryMemoryLimit(100000000);

    StartRms({config, MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 2;

    bool allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 1'000});
    UNIT_ASSERT(allocated);

    allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .Memory = 100'000});
    UNIT_ASSERT(!allocated);

    AssertResourceManagerStats(rm, config.GetQueryMemoryLimit() - 1000, 99);
    AssertResourceBrokerSensors(0, 1000, 0, 0, 1);
}

void KqpRm::Snapshot() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    NRm::TKqpResourcesRequest request{.ExecutionUnits = 10, .Memory = 100};

    {
        auto tx1 = MakeTx(1, rm);
        auto tx2 = MakeTx(2, rm);

        auto task2 = 2;
        auto task1 = 1;

        bool allocated = rm->AllocateResources(*tx1, task2, request);
        UNIT_ASSERT(allocated);

        allocated &= rm->AllocateResources(*tx2, task1, request);
        UNIT_ASSERT(allocated);

        AssertResourceManagerStats(rm, 800, 80);
        AssertResourceBrokerSensors(0, 200, 0, 0, 2);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{800, 80}, {1000, 100}}, rm);

        rm->FreeResources(*tx1, task2, request);
        rm->FreeResources(*tx2, task1, request);

        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, 0, 2);
    }

    AssertResourceBrokerSensors(0, 0, 0, 2, 0);

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm);
}

void KqpRm::SingleSnapshotByExchanger() {
    Snapshot();
}

void KqpRm::Reduce() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    auto tx = MakeTx(1, rm);
    auto task = 1;

    bool allocated = rm->AllocateResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 10, .Memory = 100});
    UNIT_ASSERT(allocated);

    AssertResourceManagerStats(rm, 1000 - 100, 100 - 10);
    AssertResourceBrokerSensors(0, 100, 0, 0, 1);

    NRm::TKqpResourcesRequest reduceRequest;
    reduceRequest.Memory = 70;

    rm->FreeResources(*tx, task, NRm::TKqpResourcesRequest{.ExecutionUnits = 7, .Memory = 70});
    AssertResourceManagerStats(rm, 1000 - 100 + 70, 100 - 10 + 7);
    AssertResourceBrokerSensors(0, 30, 0, 0, 1);
}

void KqpRm::ConcurrentTasks() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);

        NPar::LocalExecutor().RunAdditionalThreads(10);
        std::atomic<ui64> failedAllocations = 0;

        NPar::LocalExecutor().ExecRange([&](int index) {
            const int taskId = index + 1;
            auto count = 0u;
            for (auto n = 0u; n < 20u; n++) {
                for (auto j = 0u; j < 20u; j++) {
                    if (!rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = j, .Memory = j * 10u})) {
                        failedAllocations++;
                        Sleep(TDuration::MilliSeconds(j * 10));
                        break;
                    }
                    count += j;
                }
                for (auto j = 20u; j > 0u; j--) {
                    if (count < j) {
                        break;
                    }
                    rm->FreeResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = j, .Memory = j * 10u});
                    count -= j;
                }
            }
            rm->FreeResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = count, .Memory = count * 10u});
        }, 0, 10, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

        UNIT_ASSERT_GT(failedAllocations.load(), 0);
        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 1);
    }

    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

void KqpRm::ConcurrentChannels() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);

        {
            auto qm = CreateChannelQuotaManager(rm, tx, 0, 16);

            NPar::LocalExecutor().RunAdditionalThreads(10);
            std::atomic<ui64> failedAllocations = 0;

            NPar::LocalExecutor().ExecRange([&](int) {
                auto count = 0u;
                for (auto n = 0u; n < 20u; n++) {
                    for (auto j = 0u; j < 20u; j++) {
                        if (!qm->AllocateQuota(j * 10u, /* isOptional = */ false)) {
                            failedAllocations++;
                            Sleep(TDuration::MilliSeconds(j * 10));
                            break;
                        }
                        count += j;
                    }
                    for (auto j = 20u; j > 0u; j--) {
                        if (count < j) {
                            break;
                        }
                        qm->FreeQuota(j * 10u);
                        count -= j;
                    }
                }
                qm->FreeQuota(count * 10u);
            }, 0, 10, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

            UNIT_ASSERT_GT(failedAllocations.load(), 0);

            // the channel quota manager exposes the node level memory availability of its tx,
            // DQ channels 2.0 propagate a negative value to the senders as back pressure. The load above
            // drives allocations to failure, so the cookie may well be negative already - set it explicitly.
            UNIT_ASSERT(tx->TotalMemoryCookie);
            const i64 saved = tx->TotalMemoryCookie->MemoryAvailability.load();
            tx->TotalMemoryCookie->MemoryAvailability.store(0);
            const i64 base = qm->GetMemoryAvailability(); // the locally prepaid channel quota
            tx->TotalMemoryCookie->MemoryAvailability.store(500);
            UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), base + 500);
            UNIT_ASSERT(!tx->IsReasonableToStartSpilling());
            // a negative node value dominates: prepaid quota must not mask node level memory pressure
            tx->TotalMemoryCookie->MemoryAvailability.store(-1000000);
            UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), -1000000);
            UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -1000000);
            UNIT_ASSERT(tx->IsReasonableToStartSpilling());
            tx->TotalMemoryCookie->MemoryAvailability.store(saved);
        }

        AssertResourceManagerStats(rm, 1000, 100);
        AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 1);
    }

    AssertResourceBrokerSensors(0, 0, 0, std::nullopt, 0);
}

// QueryMemoryLimit = 1000 with the default SpillingPercent = 80: the spilling threshold is at 800 used,
// the availability is 800 - used and turns negative past it (the old SpillingPercentReached signal)
void KqpRm::MemoryAvailability() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        // nothing allocated yet: no cookie, nothing is known about the node
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), std::numeric_limits<i64>::max());
        UNIT_ASSERT(!tx->IsReasonableToStartSpilling());

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700);
        UNIT_ASSERT(!tx->IsReasonableToStartSpilling());

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 700}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 0); // at the threshold: not pressure yet
        UNIT_ASSERT(!tx->IsReasonableToStartSpilling());

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 1}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -1);
        UNIT_ASSERT(tx->IsReasonableToStartSpilling());

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 801});
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 800);
        UNIT_ASSERT(!tx->IsReasonableToStartSpilling());
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// A tx with a resource pool sees the minimum over the node total and its pool: the pool limit is
// MemoryPoolPercent of the node limit, the spilling threshold applies to each of them
void KqpRm::PoolMemoryAvailability() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakePoolTx(1, rm, /* memoryPoolPercent = */ 50);
        // pool limit 500, pool threshold at 400 used; node threshold at 800 used
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 100}));
        UNIT_ASSERT(tx->PoolMemoryCookie);
        UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 700);
        UNIT_ASSERT_VALUES_EQUAL(tx->PoolMemoryCookie->MemoryAvailability.load(), 300);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 300);

        UNIT_ASSERT(rm->AllocateResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 350}));
        UNIT_ASSERT_VALUES_EQUAL(tx->TotalMemoryCookie->MemoryAvailability.load(), 350);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), -50); // the pool is over its threshold
        UNIT_ASSERT(tx->IsReasonableToStartSpilling());

        rm->FreeResources(*tx, 1, NRm::TKqpResourcesRequest{.Memory = 350});
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 300);
        UNIT_ASSERT(!tx->IsReasonableToStartSpilling());
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

// The quota managers refuse optional requests in advance when the tx availability cannot cover the aligned
// step (no resource manager round trip), their availability follows the sign of the tx value, and an optional
// request that fits the availability goes to the resource manager as usual
void KqpRm::TaskQuotaManagerOptional() {
    StartRms();
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers.front().NodeId());

    {
        auto tx = MakeTx(1, rm);
        const ui64 taskId = 1;
        const ui64 initialLimit = 100;
        // the node service prepays the initial limit as external memory before the task starts
        UNIT_ASSERT(rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.ExecutionUnits = 1, .ExternalMemory = initialLimit}));
        UNIT_ASSERT(rm->AllocateResources(*tx, taskId, NRm::TKqpResourcesRequest{.Memory = 100}));
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700);
        const auto statsBefore = rm->GetLocalResources();

        // task level manager: 1 MB allocation step, the test resource broker cannot grant that much
        auto qm = CreateTaskQuotaManager(rm, tx, taskId, initialLimit);
        UNIT_ASSERT(qm->AllocateQuota(50, /* isOptional = */ true)); // fits in the prepaid limit
        UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), 700 + 50); // tx value plus the local leftover
        UNIT_ASSERT(!qm->AllocateQuota(1500, /* isOptional = */ true)); // refused in advance
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, statsBefore.Memory); // no round trip
        // a negative tx value dominates the local leftover
        tx->TotalMemoryCookie->MemoryAvailability.store(-7);
        UNIT_ASSERT_VALUES_EQUAL(qm->GetMemoryAvailability(), -7);
        tx->TotalMemoryCookie->MemoryAvailability.store(700);
        qm->FreeQuota(50);
        qm.reset();

        // channel level manager: 16 byte allocation step, the granted path is observable
        auto cm = CreateChannelQuotaManager(rm, tx, 0, 16);
        tx->TotalMemoryCookie->MemoryAvailability.store(500);
        UNIT_ASSERT(!cm->AllocateQuota(1500, /* isOptional = */ true)); // 1504 > 500: refused in advance
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, statsBefore.Memory);
        UNIT_ASSERT_VALUES_EQUAL(cm->GetMemoryAvailability(), 500); // nothing prepaid here
        tx->TotalMemoryCookie->MemoryAvailability.store(700);
        UNIT_ASSERT(cm->AllocateQuota(200, /* isOptional = */ true)); // 208 <= 700: granted by the resource manager
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, statsBefore.Memory - 208);
        UNIT_ASSERT_VALUES_EQUAL(tx->GetMemoryAvailability(), 700 - 208); // the cookie follows the allocation
        cm->FreeQuota(200);
        cm.reset();
        UNIT_ASSERT_VALUES_EQUAL(rm->GetLocalResources().Memory, statsBefore.Memory);

        rm->FreeResources(*tx, taskId, NRm::TKqpResourcesRequest{.Memory = 100});
    }

    AssertResourceManagerStats(rm, 1000, 100);
}

void KqpRm::SnapshotSharing() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm_first = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm_first);
    CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_second);

    NRm::TKqpResourcesRequest request{.ExecutionUnits = 10, .Memory = 100};

    auto tx1Rm1 = MakeTx(1, rm_first);
    auto tx2Rm1 = MakeTx(2, rm_first);
    auto task1Rm1 = 1;
    auto task2Rm1 = 2;

    auto tx1Rm2 = MakeTx(1, rm_second);
    auto tx2Rm2 = MakeTx(2, rm_second);
    auto task1Rm2 = 1;
    auto task2Rm2 = 2;

    {
        bool allocated = rm_first->AllocateResources(*tx1Rm1, task1Rm1, request);
        UNIT_ASSERT(allocated);

        allocated &= rm_first->AllocateResources(*tx2Rm1, task2Rm1, request);
        UNIT_ASSERT(allocated);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{800, 80}, {1000, 100}}, rm_second);
    }

    {
        bool allocated = rm_second->AllocateResources(*tx1Rm2, task1Rm2, request);
        UNIT_ASSERT(allocated);

        allocated &= rm_second->AllocateResources(*tx2Rm2, task2Rm2, request);
        UNIT_ASSERT(allocated);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(1, {{800, 80}, {800, 80}}, rm_first);
    }

    {
        rm_first->FreeResources(*tx1Rm1, task1Rm1, request);
        rm_first->FreeResources(*tx2Rm1, task2Rm1, request);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(0, {{1000, 100}, {800, 80}}, rm_second);
    }

    {
        rm_second->FreeResources(*tx1Rm2, task1Rm2, request);
        rm_second->FreeResources(*tx2Rm2, task2Rm2, request);

        Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_first);
    }
}

void KqpRm::SnapshotSharingByExchanger() {
    SnapshotSharing();
}

void KqpRm::NodesMembership() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm_first = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm_first);
    CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_second);

    const TActorId edge = Runtime->AllocateEdgeActor(1);
    Runtime->Send(new IEventHandle(
        ResourceManagers[1], edge, new TEvents::TEvPoison, IEventHandle::FlagTrackDelivery, 0),
        1, false);

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Poison, 1);
    UNIT_ASSERT(Runtime->DispatchEvents(options));

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}}, rm_first);
}


void KqpRm::NodesMembershipByExchanger() {
    NodesMembership();
}

void KqpRm::DisonnectNodes() {
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm_first = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto rm_second = GetKqpResourceManager(ResourceManagers[1].NodeId());

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}, {1000, 100}}, rm_first);
    CheckSnapshot(1, {{1000, 100}, {1000, 100}}, rm_second);

    auto prevObserverFunc = Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case NRm::TEvKqpResourceInfoExchanger::TEvSendResources::EventType: {
                return TTestActorRuntime::EEventAction::DROP;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    Disconnect(0, 1);

    Runtime->DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

    CheckSnapshot(0, {{1000, 100}}, rm_first);
}

void KqpRm::MemoryControllerBudget_Registers() {
    // flag-on: Bootstrap must send TEvConsumerRegister(QueryExecution) to MC
    // Register an edge actor at the MC service ID so the Send is actually delivered
    const TActorId fakemc0 = Runtime->AllocateEdgeActor(0);
    Runtime->RegisterService(NMemory::MakeMemoryControllerId(), fakemc0, 0);

    StartRms({MakeKqpResourceManagerConfigWithMC(), MakeKqpResourceManagerConfig()});
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    TAutoPtr<IEventHandle> handle;
    auto* regEv = Runtime->GrabEdgeEventRethrow<NMemory::TEvConsumerRegister>(handle, TDuration::Seconds(1));
    UNIT_ASSERT(regEv);
    UNIT_ASSERT_VALUES_EQUAL((int)regEv->Kind, (int)NMemory::EMemoryConsumerKind::QueryExecution);

    // flag-off: no TEvConsumerRegister sent
    TearDown();
    SetUp();
    const TActorId fakemc1 = Runtime->AllocateEdgeActor(0);
    Runtime->RegisterService(NMemory::MakeMemoryControllerId(), fakemc1, 0);

    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
    // No TEvConsumerRegister expected; grab with short timeout and assert null
    TAutoPtr<IEventHandle> handle2;
    auto* regEv2 = Runtime->GrabEdgeEventRethrow<NMemory::TEvConsumerRegister>(handle2, TDuration::MilliSeconds(50));
    UNIT_ASSERT(!regEv2);
}

void KqpRm::MemoryControllerBudget_FlagOff() {
    // flag=false (default): TEvConsumerLimit must be ignored, limit stays at initial QueryMemoryLimit=1000
    StartRms({MakeKqpResourceManagerConfig(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers[0].NodeId());
    auto stats = rm->GetLocalResources();
    UNIT_ASSERT_VALUES_EQUAL(1000, stats.Memory);

    const TActorId sender = Runtime->AllocateEdgeActor(0);
    Runtime->Send(new IEventHandle(ResourceManagers[0], sender,
        new NMemory::TEvConsumerLimit(500)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    // limit unchanged: flag-off path ignores TEvConsumerLimit
    stats = rm->GetLocalResources();
    UNIT_ASSERT_VALUES_EQUAL(1000, stats.Memory);
}

void KqpRm::MemoryControllerBudget_DynamicLimit() {
    // flag-on: TEvConsumerLimit(500) must lower TotalMemoryResource limit to 500
    // Inject MC limit via a fake MC actor at the service id so sender check passes
    const TActorId fakemc = Runtime->AllocateEdgeActor(0);
    Runtime->RegisterService(NMemory::MakeMemoryControllerId(), fakemc, 0);

    StartRms({MakeKqpResourceManagerConfigWithMC(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers[0].NodeId());
    // Grab TEvConsumerRegister, reply with TEvConsumerRegistered so RM records McActorId = fakemc
    TAutoPtr<IEventHandle> regHandle;
    Runtime->GrabEdgeEventRethrow<NMemory::TEvConsumerRegister>(regHandle, TDuration::Seconds(1));
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerRegistered(nullptr)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    // Inject MC limit 500 from the fake MC (passes sender check)
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerLimit(500)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    // Available = limit(500) - used(0) = 500; allocate 400 must succeed, 401 must fail
    auto tx400 = MakeTx(1, rm);
    UNIT_ASSERT(rm->AllocateResources(*tx400, 1, NRm::TKqpResourcesRequest{.Memory = 400}));

    auto tx401 = MakeTx(2, rm);
    UNIT_ASSERT(!rm->AllocateResources(*tx401, 2, NRm::TKqpResourcesRequest{.Memory = 401}));

    rm->FreeResources(*tx400, 1, NRm::TKqpResourcesRequest{.Memory = 400});
}

void KqpRm::MemoryControllerBudget_RbConfigLogOnly() {
    // flag-on: after TEvConsumerLimit received, TEvConfigResponse from RB must be log-only (not change limit)
    const TActorId fakemc = Runtime->AllocateEdgeActor(0);
    Runtime->RegisterService(NMemory::MakeMemoryControllerId(), fakemc, 0);

    StartRms({MakeKqpResourceManagerConfigWithMC(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers[0].NodeId());
    TAutoPtr<IEventHandle> regHandle;
    Runtime->GrabEdgeEventRethrow<NMemory::TEvConsumerRegister>(regHandle, TDuration::Seconds(1));
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerRegistered(nullptr)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    // Inject MC limit 500 from the fake MC (passes sender check)
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerLimit(500)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    auto stats = rm->GetLocalResources();
    UNIT_ASSERT_VALUES_EQUAL(500, stats.Memory);

    // Inject a simulated TEvConfigResponse with memory=900 — RM must ignore it (MC-driven)
    {
        auto* rbResp = new TEvResourceBroker::TEvConfigResponse;
        TQueueConfig qc;
        qc.SetName(NLocalDb::KqpResourceManagerQueue);
        qc.MutableLimit()->SetMemory(900);
        rbResp->QueueConfig = qc;
        const TActorId rbSender = Runtime->AllocateEdgeActor(0);
        Runtime->Send(new IEventHandle(ResourceManagers[0], rbSender, rbResp), 0, true);
    }
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    // limit must still be 500 (RB config ignored once MC-driven)
    stats = rm->GetLocalResources();
    UNIT_ASSERT_VALUES_EQUAL(500, stats.Memory);
}

void KqpRm::MemoryControllerBudget_RbConfigIgnoredEvenWhenValuesAgree() {
    // Once MC-driven, TEvConfigResponse from RB must be ignored even when its value matches the MC limit.
    // Registers a fake MC at service ID so the sender check passes.
    const TActorId fakemc = Runtime->AllocateEdgeActor(0);
    Runtime->RegisterService(NMemory::MakeMemoryControllerId(), fakemc, 0);

    StartRms({MakeKqpResourceManagerConfigWithMC(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers[0].NodeId());
    TAutoPtr<IEventHandle> regHandle;
    Runtime->GrabEdgeEventRethrow<NMemory::TEvConsumerRegister>(regHandle, TDuration::Seconds(1));
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerRegistered(nullptr)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    constexpr ui64 McLimit = 500;

    // MC delivers a limit; RM becomes MC-driven
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerLimit(McLimit)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    auto stats = rm->GetLocalResources();
    UNIT_ASSERT_VALUES_EQUAL_C(McLimit, stats.Memory,
        "TotalMemoryResource must equal consumer limit after TEvConsumerLimit");

    // RB config arrives with the same value — RM must ignore it (MC-driven)
    {
        auto* rbResp = new TEvResourceBroker::TEvConfigResponse;
        TQueueConfig qc;
        qc.SetName(NLocalDb::KqpResourceManagerQueue);
        qc.MutableLimit()->SetMemory(McLimit);
        rbResp->QueueConfig = qc;
        const TActorId sender = Runtime->AllocateEdgeActor(0);
        Runtime->Send(new IEventHandle(ResourceManagers[0], sender, rbResp), 0, true);
    }
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    stats = rm->GetLocalResources();
    UNIT_ASSERT_VALUES_EQUAL_C(McLimit, stats.Memory,
        "TotalMemoryResource must remain at McLimit after RB config with same value (MC-driven)");
}

void KqpRm::MemoryControllerBudget_StalenessRevert() {
    // After McLimitStalenessTimeout without TEvConsumerLimit, RM must revert to RB-driven.
    // After revert, a subsequent TEvConfigResponse must take effect.
    const TActorId fakemc = Runtime->AllocateEdgeActor(0);
    Runtime->RegisterService(NMemory::MakeMemoryControllerId(), fakemc, 0);

    StartRms({MakeKqpResourceManagerConfigWithMC(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers[0].NodeId());
    TAutoPtr<IEventHandle> regHandle;
    Runtime->GrabEdgeEventRethrow<NMemory::TEvConsumerRegister>(regHandle, TDuration::Seconds(1));
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerRegistered(nullptr)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    // Deliver an RB config so LastRbLimitBytes is cached
    {
        auto* rbResp = new TEvResourceBroker::TEvConfigResponse;
        TQueueConfig qc;
        qc.SetName(NLocalDb::KqpResourceManagerQueue);
        qc.MutableLimit()->SetMemory(700);
        rbResp->QueueConfig = qc;
        const TActorId sender = Runtime->AllocateEdgeActor(0);
        Runtime->Send(new IEventHandle(ResourceManagers[0], sender, rbResp), 0, true);
    }
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    // MC delivers a limit; RM becomes MC-driven
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerLimit(500)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    UNIT_ASSERT_VALUES_EQUAL(500, rm->GetLocalResources().Memory);

    // Advance past McLimitStalenessTimeout (10s) without sending another TEvConsumerLimit
    Runtime->AdvanceCurrentTime(TDuration::Seconds(11));
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(200));

    // After revert, RM applies the last cached RB limit (700)
    UNIT_ASSERT_VALUES_EQUAL(700, rm->GetLocalResources().Memory);

    // A fresh TEvConfigResponse now takes effect (no longer MC-driven)
    {
        auto* rbResp = new TEvResourceBroker::TEvConfigResponse;
        TQueueConfig qc;
        qc.SetName(NLocalDb::KqpResourceManagerQueue);
        qc.MutableLimit()->SetMemory(800);
        rbResp->QueueConfig = qc;
        const TActorId sender = Runtime->AllocateEdgeActor(0);
        Runtime->Send(new IEventHandle(ResourceManagers[0], sender, rbResp), 0, true);
    }
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    UNIT_ASSERT_VALUES_EQUAL(800, rm->GetLocalResources().Memory);
}

void KqpRm::MemoryControllerBudget_RecoveryAfterRevert() {
    // After a staleness revert, a new TEvConsumerLimit re-enters MC-driven mode and arms the watchdog.
    // A second staleness revert must also work.
    const TActorId fakemc = Runtime->AllocateEdgeActor(0);
    Runtime->RegisterService(NMemory::MakeMemoryControllerId(), fakemc, 0);

    StartRms({MakeKqpResourceManagerConfigWithMC(), MakeKqpResourceManagerConfig()});
    NKikimr::TActorSystemStub stub;

    auto rm = GetKqpResourceManager(ResourceManagers[0].NodeId());
    TAutoPtr<IEventHandle> regHandle;
    Runtime->GrabEdgeEventRethrow<NMemory::TEvConsumerRegister>(regHandle, TDuration::Seconds(1));
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerRegistered(nullptr)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    // Deliver RB limit so LastRbLimitBytes is cached
    {
        auto* rbResp = new TEvResourceBroker::TEvConfigResponse;
        TQueueConfig qc;
        qc.SetName(NLocalDb::KqpResourceManagerQueue);
        qc.MutableLimit()->SetMemory(700);
        rbResp->QueueConfig = qc;
        const TActorId sender = Runtime->AllocateEdgeActor(0);
        Runtime->Send(new IEventHandle(ResourceManagers[0], sender, rbResp), 0, true);
    }
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

    // First MC limit: MC-driven
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerLimit(500)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
    UNIT_ASSERT_VALUES_EQUAL(500, rm->GetLocalResources().Memory);

    // First revert
    Runtime->AdvanceCurrentTime(TDuration::Seconds(11));
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(200));
    UNIT_ASSERT_VALUES_EQUAL(700, rm->GetLocalResources().Memory);

    // Re-enter MC-driven mode with a new TEvConsumerLimit
    Runtime->Send(new IEventHandle(ResourceManagers[0], fakemc,
        new NMemory::TEvConsumerLimit(600)), 0, true);
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
    UNIT_ASSERT_VALUES_EQUAL(600, rm->GetLocalResources().Memory);

    // Second revert: watchdog must have been re-armed after recovery
    Runtime->AdvanceCurrentTime(TDuration::Seconds(11));
    Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(200));
    UNIT_ASSERT_VALUES_EQUAL(700, rm->GetLocalResources().Memory);
}

} // namespace NKqp
} // namespace NKikimr
