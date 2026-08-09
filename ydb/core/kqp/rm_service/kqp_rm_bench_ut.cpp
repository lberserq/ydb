#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/tablet/resource_broker_impl.h>

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/kqp/common/simple/services.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/local_executor/local_executor.h>

#include <yql/essentials/minikql/mkql_alloc.h>

#include <util/system/hp_timer.h>
#include <util/generic/algorithm.h>

#include <cmath>

namespace NKikimr {
namespace NKqp {

using namespace NKikimrResourceBroker;
using namespace NResourceBroker;

namespace {

constexpr ui32 BENCH_ITERATIONS = 100;
constexpr ui32 OPS_PER_ITERATION = 2000;
constexpr ui64 OP_MEMORY = 1_MB;

TTenantTestConfig MakeBenchTenantTestConfig() {
    TTenantTestConfig cfg = {
        {{{DOMAIN1_NAME, SCHEME_SHARD1_ID, {{TENANT1_1_NAME, TENANT1_2_NAME}}}}},
        HIVE_ID,
        true,
        true,
        false,
        {{
             {
                 {
                     {{{DOMAIN1_NAME, {1, 1, 1}}}},
                     "node-type"
                 }
             }
         }},
        1
    };
    return cfg;
}

TResourceBrokerConfig MakeBenchResourceBrokerConfig() {
    TResourceBrokerConfig config;

    auto queue = config.AddQueues();
    queue->SetName("queue_default");
    queue->SetWeight(5);
    queue->MutableLimit()->AddResource(4);

    queue = config.AddQueues();
    queue->SetName("queue_kqp_resource_manager");
    queue->SetWeight(20);
    queue->MutableLimit()->AddResource(64);
    queue->MutableLimit()->AddResource(4_GB);

    auto task = config.AddTasks();
    task->SetName("unknown");
    task->SetQueueName("queue_default");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    task = config.AddTasks();
    task->SetName(NLocalDb::KqpResourceManagerTaskName);
    task->SetQueueName("queue_kqp_resource_manager");
    task->SetDefaultDuration(TDuration::Seconds(5).GetValue());

    config.MutableResourceLimit()->AddResource(128);
    config.MutableResourceLimit()->AddResource(8_GB);

    return config;
}

NKikimrConfig::TTableServiceConfig::TResourceManager MakeBenchRmConfig() {
    NKikimrConfig::TTableServiceConfig::TResourceManager config;

    config.SetComputeActorsCount(1000);
    config.SetPublishStatisticsIntervalSec(0);
    config.SetQueryMemoryLimit(2_GB);

    auto* infoExchangerRetrySettings = config.MutableInfoExchangerSettings();
    auto* exchangerSettings = infoExchangerRetrySettings->MutableExchangerSettings();
    exchangerSettings->SetStartDelayMs(50);
    exchangerSettings->SetMaxDelayMs(50);

    return config;
}

void ReportStats(const TString& name, TVector<double>& nsPerOp) {
    Sort(nsPerOp);
    double sum = 0, sq = 0;
    for (double v : nsPerOp) {
        sum += v;
        sq += v * v;
    }
    const double n = nsPerOp.size();
    const double mean = sum / n;
    const double stddev = std::sqrt(std::max(0.0, sq / n - mean * mean));
    Cerr << "BENCHSUMMARY name=" << name
        << " iters=" << nsPerOp.size()
        << " mean_ns=" << mean
        << " p50_ns=" << nsPerOp[nsPerOp.size() / 2]
        << " p95_ns=" << nsPerOp[size_t(n * 0.95)]
        << " p99_ns=" << nsPerOp[size_t(n * 0.99)]
        << " max_ns=" << nsPerOp.back()
        << " std_ns=" << stddev
        << Endl;
}

} // namespace

class KqpRmBench : public TTestBase {
public:
    void SetUp() override {
        Runtime = MakeHolder<TTenantTestRuntime>(MakeBenchTenantTestConfig());
        Runtime->SetLogPriority(NKikimrServices::RESOURCE_BROKER, NLog::PRI_ERROR);
        Runtime->SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NLog::PRI_ERROR);

        Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();

        auto broker = CreateResourceBrokerActor(MakeBenchResourceBrokerConfig(), Counters);
        ResourceBroker = Runtime->Register(broker, 0);
        WaitForBootstrap();

        auto kqpCounters = MakeIntrusive<TKqpCounters>(Counters);
        auto resman = CreateKqpResourceManagerActor(
            MakeBenchRmConfig(), kqpCounters, ResourceBroker, nullptr, Runtime->GetNodeId(0));
        auto prevObserver = Runtime->SetRegistrationObserverFunc(
            [](TTestActorRuntimeBase& runtime, const TActorId&, const TActorId& actorId) {
                runtime.EnableScheduleForActor(actorId, true);
            });
        ResourceManager = Runtime->Register(resman, 0);
        Runtime->RegisterService(
            MakeKqpResourceManagerServiceID(Runtime->GetNodeId(0)), ResourceManager, 0);
        Runtime->SetRegistrationObserverFunc(prevObserver);
        WaitForBootstrap();
    }

    void TearDown() override {
        Runtime.Reset();
    }

    void WaitForBootstrap() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(Runtime->DispatchEvents(options));
    }

    TIntrusivePtr<NRm::TTxState> MakeBenchTx(
            ui64 txId, std::shared_ptr<NRm::IKqpResourceManager> rm,
            const TString& poolId, double percent) {
        return MakeIntrusive<NRm::TTxState>(
            rm, txId, TInstant::Now(), poolId, percent, "benchdb", false);
    }

    // Steady-state alloc/free latency: one base task keeps the tx (and named
    // pool entry) alive, the measured loop allocates and frees OP_MEMORY.
    void RunAllocFreeBench(const TString& name, const TString& poolId, double percent) {
        NKikimr::TActorSystemStub stub;
        auto rm = GetKqpResourceManager(ResourceManager.NodeId());

        auto tx = MakeBenchTx(1, rm, poolId, percent);
        NRm::TKqpResourcesRequest base{.Memory = OP_MEMORY};
        UNIT_ASSERT(rm->AllocateResources(*tx, 1, base));

        NRm::TKqpResourcesRequest request{.Memory = OP_MEMORY};
        TVector<double> nsPerOp;
        nsPerOp.reserve(BENCH_ITERATIONS);

        for (ui32 iter = 0; iter < BENCH_ITERATIONS; ++iter) {
            THPTimer timer;
            for (ui32 op = 0; op < OPS_PER_ITERATION; ++op) {
                bool ok = rm->AllocateResources(*tx, 2, request);
                Y_ABORT_UNLESS(ok);
                rm->FreeResources(*tx, 2, request);
            }
            nsPerOp.push_back(timer.Passed() * 1e9 / OPS_PER_ITERATION);
        }

        rm->FreeResources(*tx, 1, base);
        ReportStats(name, nsPerOp);
    }

    void AllocNoPool() {
        RunAllocFreeBench("rm_alloc_free_no_pool", "", 100);
    }

    // ExternalMemory-only path: today lock-free (two atomics), the planned D6
    // default routes it through pool accounting — measure the baseline.
    void AllocExternalOnly() {
        NKikimr::TActorSystemStub stub;
        auto rm = GetKqpResourceManager(ResourceManager.NodeId());

        auto tx = MakeBenchTx(2, rm, "benchpool", 50);
        NRm::TKqpResourcesRequest request{.ExternalMemory = OP_MEMORY};
        TVector<double> nsPerOp;
        nsPerOp.reserve(BENCH_ITERATIONS);

        for (ui32 iter = 0; iter < BENCH_ITERATIONS; ++iter) {
            THPTimer timer;
            for (ui32 op = 0; op < OPS_PER_ITERATION; ++op) {
                bool ok = rm->AllocateResources(*tx, 2, request);
                Y_ABORT_UNLESS(ok);
                rm->FreeResources(*tx, 2, request);
            }
            nsPerOp.push_back(timer.Passed() * 1e9 / OPS_PER_ITERATION);
        }
        ReportStats("rm_alloc_free_external_only", nsPerOp);
    }

    void AllocNamedPool() {
        RunAllocFreeBench("rm_alloc_free_named_pool", "benchpool", 50);
    }

    // N threads, each with its own tx in the same named pool.
    void RunContendedBench(ui32 THREADS, const TString& name) {
        NKikimr::TActorSystemStub stub;
        auto rm = GetKqpResourceManager(ResourceManager.NodeId());

        TVector<TIntrusivePtr<NRm::TTxState>> txs;
        NRm::TKqpResourcesRequest base{.Memory = OP_MEMORY};
        for (ui32 t = 0; t < THREADS; ++t) {
            txs.push_back(MakeBenchTx(100 + t, rm, "benchpool", 50));
            UNIT_ASSERT(rm->AllocateResources(*txs.back(), 1, base));
        }

        TVector<TVector<double>> perThread(THREADS);
        NPar::TLocalExecutor executor;
        executor.RunAdditionalThreads(THREADS);

        for (ui32 iter = 0; iter < BENCH_ITERATIONS; ++iter) {
            executor.ExecRange([&](int t) {
                NKikimr::TActorSystemStub threadStub;
                NRm::TKqpResourcesRequest request{.Memory = OP_MEMORY};
                THPTimer timer;
                for (ui32 op = 0; op < OPS_PER_ITERATION; ++op) {
                    bool ok = rm->AllocateResources(*txs[t], 2, request);
                    Y_ABORT_UNLESS(ok);
                    rm->FreeResources(*txs[t], 2, request);
                }
                perThread[t].push_back(timer.Passed() * 1e9 / OPS_PER_ITERATION);
            }, 0, THREADS, NPar::TLocalExecutor::WAIT_COMPLETE);
        }

        for (ui32 t = 0; t < THREADS; ++t) {
            rm->FreeResources(*txs[t], 1, base);
        }

        TVector<double> all;
        for (auto& v : perThread) {
            all.insert(all.end(), v.begin(), v.end());
        }
        ReportStats(name, all);
    }

    void AllocNamedPoolContended4() {
        RunContendedBench(4, "rm_alloc_free_named_pool_contended4");
    }

    void AllocNamedPoolContended16() {
        RunContendedBench(16, "rm_alloc_free_named_pool_contended16");
    }

    void AllocNamedPoolContended32() {
        RunContendedBench(32, "rm_alloc_free_named_pool_contended32");
    }

    // OLTP-shaped: the full RM lifecycle of a small query — tx creation,
    // one ExecutionUnits+ExternalMemory reservation, free, tx destruction.
    void OltpQueryCycle() {
        NKikimr::TActorSystemStub stub;
        auto rm = GetKqpResourceManager(ResourceManager.NodeId());

        NRm::TKqpResourcesRequest request{.ExecutionUnits = 1, .ExternalMemory = OP_MEMORY};
        TVector<double> nsPerOp;
        nsPerOp.reserve(BENCH_ITERATIONS);
        ui64 txId = 1000;

        for (ui32 iter = 0; iter < BENCH_ITERATIONS; ++iter) {
            THPTimer timer;
            for (ui32 op = 0; op < OPS_PER_ITERATION; ++op) {
                auto tx = MakeBenchTx(++txId, rm, "benchpool", 50);
                bool ok = rm->AllocateResources(*tx, 1, request);
                Y_ABORT_UNLESS(ok);
                rm->FreeResources(*tx, 1, request);
            }
            nsPerOp.push_back(timer.Passed() * 1e9 / OPS_PER_ITERATION);
        }
        ReportStats("rm_oltp_query_cycle", nsPerOp);
    }

    // MKQL page pool churn: mixed-size allocations, free odd, second wave,
    // free all. Reports latency and post-churn retained-vs-used memory.
    void MkqlChurn(const TString& name, bool tightLimitWithCallback) {
        constexpr ui32 BLOCKS = 8192;
        static constexpr size_t SIZES[] = {24, 256, 4096, 65536 - 64};

        TVector<double> nsPerOp;
        TVector<double> retainedRatio;
        ui64 lastAllocated = 0, lastUsed = 0, lastFreePages = 0;
        ui64 midAllocated = 0, midUsed = 0;

        for (ui32 iter = 0; iter < BENCH_ITERATIONS; ++iter) {
            NMiniKQL::TScopedAlloc alloc(__LOCATION__);
            if (tightLimitWithCallback) {
                alloc.SetLimit(8_MB);
                alloc.Ref().SetIncreaseMemoryLimitCallback([&](ui64 /*limit*/, ui64 required) {
                    alloc.SetLimit(required + 32_MB);
                });
            } else {
                alloc.SetLimit(2_GB);
            }

            TVector<std::pair<void*, size_t>> blocks(BLOCKS, {nullptr, 0});
            ui64 ops = 0;
            THPTimer timer;

            {
                for (ui32 i = 0; i < BLOCKS; ++i) {
                    size_t sz = SIZES[i % Y_ARRAY_SIZE(SIZES)];
                    blocks[i] = {NMiniKQL::TWithMiniKQLAlloc<>::AllocWithSize(sz), sz};
                    ++ops;
                }
                for (ui32 i = 0; i < BLOCKS; i += 2) {
                    NMiniKQL::TWithMiniKQLAlloc<>::FreeWithSize(blocks[i].first, blocks[i].second);
                    blocks[i].first = nullptr;
                    ++ops;
                }
                midAllocated = alloc.GetAllocated();
                midUsed = alloc.GetUsed();
                for (ui32 i = 0; i < BLOCKS; i += 2) {
                    size_t sz = SIZES[(i + 1) % Y_ARRAY_SIZE(SIZES)];
                    blocks[i] = {NMiniKQL::TWithMiniKQLAlloc<>::AllocWithSize(sz), sz};
                    ++ops;
                }
            }

            lastAllocated = alloc.GetAllocated();
            lastUsed = alloc.GetUsed();
            lastFreePages = alloc.Ref().GetFreePageCount();
            retainedRatio.push_back(midUsed ? double(midAllocated) / midUsed : 0);

            {
                for (ui32 i = 0; i < BLOCKS; ++i) {
                    if (blocks[i].first) {
                        NMiniKQL::TWithMiniKQLAlloc<>::FreeWithSize(blocks[i].first, blocks[i].second);
                        ++ops;
                    }
                }
            }
            nsPerOp.push_back(timer.Passed() * 1e9 / ops);
        }

        ReportStats(name, nsPerOp);
        Sort(retainedRatio);
        Cerr << "BENCHFRAG name=" << name
            << " steady_allocated_bytes=" << lastAllocated
            << " steady_used_bytes=" << lastUsed
            << " steady_free_pages=" << lastFreePages
            << " after_free_half_allocated_bytes=" << midAllocated
            << " after_free_half_used_bytes=" << midUsed
            << " retained_over_used_p50=" << retainedRatio[retainedRatio.size() / 2]
            << " retained_over_used_max=" << retainedRatio.back()
            << Endl;
    }

    void MkqlChurnPlain() {
        MkqlChurn("mkql_churn_plain_limit", false);
    }

    void MkqlChurnCallback() {
        MkqlChurn("mkql_churn_callback_limit", true);
    }

    UNIT_TEST_SUITE(KqpRmBench);
        UNIT_TEST(AllocNoPool);
        UNIT_TEST(AllocExternalOnly);
        UNIT_TEST(AllocNamedPool);
        UNIT_TEST(AllocNamedPoolContended4);
        UNIT_TEST(AllocNamedPoolContended16);
        UNIT_TEST(AllocNamedPoolContended32);
        UNIT_TEST(OltpQueryCycle);
        UNIT_TEST(MkqlChurnPlain);
        UNIT_TEST(MkqlChurnCallback);
    UNIT_TEST_SUITE_END();

private:
    THolder<TTestBasicRuntime> Runtime;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TActorId ResourceBroker;
    TActorId ResourceManager;
};
UNIT_TEST_SUITE_REGISTRATION(KqpRmBench);

} // namespace NKqp
} // namespace NKikimr
