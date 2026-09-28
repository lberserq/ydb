#include <ydb/core/tx/limiter/grouped_memory/service/counters.h>
#include <ydb/core/tx/limiter/grouped_memory/service/manager.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/object_counter.h>

Y_UNIT_TEST_SUITE(GroupedMemoryLimiter) {
    using namespace NKikimr;

    class TAllocation: public NOlap::NGroupedMemoryManager::IAllocation, public TObjectCounter<TAllocation> {
    public:
        std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard> Guard;
    private:
        using TBase = NOlap::NGroupedMemoryManager::IAllocation;
        virtual void DoOnAllocationImpossible(const TString& errorMessage) override {
            AFL_VERIFY(false)("error", errorMessage);
        }

        virtual bool DoOnAllocated(std::shared_ptr<NOlap::NGroupedMemoryManager::TAllocationGuard>&& guard,
            const std::shared_ptr<NOlap::NGroupedMemoryManager::IAllocation>& /*allocation*/) override {
            Guard = std::move(guard);
            return true;
        }

    public:
        TAllocation(const ui64 mem)
            : TBase(mem) {
        }
    };

    Y_UNIT_TEST(Simplest) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto groupedMemoryLimiterCounters = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, groupedMemoryLimiterCounters->BuildStageCounters("general"));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            auto alloc1 = std::make_shared<TAllocation>(50);
            manager->RegisterProcess(0, {});
            manager->RegisterProcessScope(0, 0);
            manager->RegisterGroup(0, 0, 1);
            manager->RegisterAllocation(0, 0, 1, alloc1, {});
            UNIT_ASSERT(alloc1->IsAllocated());
            auto alloc1_1 = std::make_shared<TAllocation>(50);
            manager->RegisterAllocation(0, 0, 1, alloc1_1, {});
            UNIT_ASSERT(alloc1_1->IsAllocated());

            manager->RegisterGroup(0, 0, 2);
            auto alloc2 = std::make_shared<TAllocation>(50);
            manager->RegisterAllocation(0, 0, 2, alloc2, {});
            UNIT_ASSERT(!alloc2->IsAllocated());
            alloc1->Guard.reset();
            manager->UnregisterAllocation(0, 0, alloc1->GetIdentifier());

            UNIT_ASSERT(alloc2->IsAllocated());
            manager->UnregisterAllocation(0, 0, alloc2->GetIdentifier());
            manager->UnregisterAllocation(0, 0, alloc1_1->GetIdentifier());
            manager->UnregisterGroup(0, 0, 1);
            manager->UnregisterGroup(0, 0, 2);
            manager->UnregisterProcessScope(0, 0);
            manager->UnregisterProcess(0);
        }
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(Simple) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto groupedMemoryLimiterCounters = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, groupedMemoryLimiterCounters->BuildStageCounters("general"));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            manager->RegisterProcess(0, {});
            manager->RegisterProcessScope(0, 0);
            auto alloc1 = std::make_shared<TAllocation>(10);
            manager->RegisterGroup(0, 0, 1);
            manager->RegisterAllocation(0, 0, 1, alloc1, {});
            UNIT_ASSERT(alloc1->IsAllocated());
            auto alloc2 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 0, 2);
            manager->RegisterAllocation(0, 0, 2, alloc2, {});
            UNIT_ASSERT(!alloc2->IsAllocated());
            auto alloc3 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 0, 3);
            manager->RegisterAllocation(0, 0, 3, alloc3, {});
            UNIT_ASSERT(alloc1->IsAllocated());
            UNIT_ASSERT(!alloc2->IsAllocated());
            UNIT_ASSERT(!alloc3->IsAllocated());
            auto alloc1_1 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 0, 1, alloc1_1, {});
            UNIT_ASSERT(alloc1_1->IsAllocated());
            UNIT_ASSERT(!alloc2->IsAllocated());
            alloc1_1->ResetAllocation();
            manager->UnregisterAllocation(0, 0, alloc1_1->GetIdentifier());
            UNIT_ASSERT(!alloc2->IsAllocated());
            manager->UnregisterGroup(0, 0, 1);
            UNIT_ASSERT(alloc2->IsAllocated());

            manager->UnregisterAllocation(0, 0, alloc1->GetIdentifier());
            UNIT_ASSERT(!alloc3->IsAllocated());
            manager->UnregisterGroup(0, 0, 2);
            manager->UnregisterAllocation(0, 0, alloc2->GetIdentifier());
            UNIT_ASSERT(alloc3->IsAllocated());
            manager->UnregisterGroup(0, 0, 3);
            manager->UnregisterAllocation(0, 0, alloc3->GetIdentifier());
            manager->UnregisterProcessScope(0, 0);
            manager->UnregisterProcess(0);
        }
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(CommonUsage) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto groupedMemoryLimiterCounters = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, groupedMemoryLimiterCounters->BuildStageCounters("general"));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            manager->RegisterProcess(0, {});
            manager->RegisterProcessScope(0, 0);
            manager->RegisterGroup(0, 0, 1);
            auto alloc0 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 0, 1, alloc0, {});
            auto alloc1 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 0, 1, alloc1, {});
            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(alloc1->IsAllocated());

            manager->RegisterGroup(0, 0, 2);
            auto alloc2 = std::make_shared<TAllocation>(1000);
            manager->RegisterAllocation(0, 0, 2, alloc0, {});
            manager->RegisterAllocation(0, 0, 2, alloc2, {});
            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(!alloc2->IsAllocated());

            auto alloc3 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 0, 3);
            manager->RegisterAllocation(0, 0, 3, alloc0, {});
            manager->RegisterAllocation(0, 0, 3, alloc3, {});
            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(alloc1->IsAllocated());
            UNIT_ASSERT(!alloc2->IsAllocated());
            UNIT_ASSERT(!alloc3->IsAllocated());

            manager->UnregisterGroup(0, 0, 1);
            manager->UnregisterAllocation(0, 0, alloc1->GetIdentifier());

            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(alloc2->IsAllocated());
            UNIT_ASSERT(!alloc3->IsAllocated());
            manager->UnregisterGroup(0, 0, 2);
            manager->UnregisterAllocation(0, 0, alloc2->GetIdentifier());
            UNIT_ASSERT(alloc0->IsAllocated());
            UNIT_ASSERT(alloc3->IsAllocated());

            manager->UnregisterGroup(0, 0, 3);
            manager->UnregisterAllocation(0, 0, alloc3->GetIdentifier());
            manager->UnregisterAllocation(0, 0, alloc0->GetIdentifier());
            manager->UnregisterProcess(0);
        }
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(Update) {
        auto counters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test");
        NOlap::NGroupedMemoryManager::TConfig config;
        {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(100);
            UNIT_ASSERT(config.DeserializeFromProto(protoConfig));
        }
        std::unique_ptr<NActors::IActor> actor(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(config, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        auto groupedMemoryLimiterCounters = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NKikimr::NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", config.GetMemoryLimit(), config.GetHardMemoryLimit(), nullptr, groupedMemoryLimiterCounters->BuildStageCounters("general"));
        auto manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), config, "test", counters, stage);
        {
            manager->RegisterProcess(0, {});
            manager->RegisterProcessScope(0, 0);
            auto alloc1 = std::make_shared<TAllocation>(1000);
            manager->RegisterGroup(0, 0, 1);
            manager->RegisterAllocation(0, 0, 1, alloc1, {});
            UNIT_ASSERT(alloc1->IsAllocated());
            auto alloc2 = std::make_shared<TAllocation>(10);
            manager->RegisterGroup(0, 0, 3);
            manager->RegisterAllocation(0, 0, 3, alloc2, {});
            UNIT_ASSERT(!alloc2->IsAllocated());

            alloc1->Guard->Update(10);
            manager->AllocationUpdated(0, 0, alloc1->GetIdentifier());
            UNIT_ASSERT(alloc2->IsAllocated());

            manager->UnregisterGroup(0, 0, 3);
            manager->UnregisterAllocation(0, 0, alloc2->GetIdentifier());

            manager->UnregisterGroup(0, 0, 1);
            manager->UnregisterAllocation(0, 0, alloc1->GetIdentifier());
            manager->UnregisterProcess(0);
        }
        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT(manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(UnregisterScopeKeepsWaitingWhenScopeHasLinks) {
        auto groupedMemoryLimiterCounters = std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(
            MakeIntrusive<NMonitoring::TDynamicCounters>(), "Scan");
        auto stage = std::make_shared<NOlap::NGroupedMemoryManager::TStageFeatures>("GLOBAL", 100, std::nullopt, nullptr,
            groupedMemoryLimiterCounters->BuildStageCounters("general"));
        NOlap::NGroupedMemoryManager::TProcessMemory process(0, 1, NActors::TActorId(), true, {}, stage);

        process.RegisterScope(0);
        process.RegisterScope(0);

        process.RegisterGroup(0, 1);
        auto alloc1 = std::make_shared<TAllocation>(100);
        process.RegisterAllocation(0, 1, alloc1, {});
        UNIT_ASSERT(alloc1->IsAllocated());

        process.RegisterGroup(0, 2);
        auto alloc2 = std::make_shared<TAllocation>(50);
        process.RegisterAllocation(0, 2, alloc2, {});
        UNIT_ASSERT(!alloc2->IsAllocated());
        UNIT_ASSERT(process.HasWaitingAllocations());

        process.UnregisterScope(0);
        UNIT_ASSERT(process.HasWaitingAllocations());

        alloc1->Guard.reset();
        UNIT_ASSERT(process.TryAllocateWaiting(1));
        UNIT_ASSERT(alloc2->IsAllocated());
        
        alloc2->Guard.reset();
        process.UnregisterAllocation(0, alloc1->GetIdentifier());
        process.UnregisterAllocation(0, alloc2->GetIdentifier());
        process.UnregisterGroup(0, 1);
        process.UnregisterGroup(0, 2);
        process.UnregisterScope(0);

        alloc1.reset();
        alloc2.reset();

        UNIT_ASSERT_VALUES_EQUAL(stage->GetUsage().Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    class TFailableAllocation: public TAllocation {
    public:
        bool Failed = false;

    private:
        virtual void DoOnAllocationImpossible(const TString& /*errorMessage*/) override {
            Failed = true;
        }

    public:
        using TAllocation::TAllocation;
    };

    struct TSlotsEnv {
        std::shared_ptr<NOlap::NGroupedMemoryManager::TCounters> Counters;
        NOlap::NGroupedMemoryManager::TConfig Config;
        std::shared_ptr<NOlap::NGroupedMemoryManager::TStageFeatures> Stage;
        std::shared_ptr<NOlap::NGroupedMemoryManager::TManager> Manager;

        TSlotsEnv(const ui32 slotsCount, const ui64 limit, const std::optional<ui64>& hardLimit, const std::optional<ui64>& slotLimit)
            : Counters(std::make_shared<NOlap::NGroupedMemoryManager::TCounters>(MakeIntrusive<NMonitoring::TDynamicCounters>(), "test")) {
            NKikimrConfig::TGroupedMemoryLimiterConfig protoConfig;
            protoConfig.SetMemoryLimit(limit);
            protoConfig.SetSlotsCount(slotsCount);
            UNIT_ASSERT(Config.DeserializeFromProto(protoConfig));
            Stage = std::make_shared<NOlap::NGroupedMemoryManager::TStageFeatures>(
                "GLOBAL", limit, hardLimit, nullptr, Counters->BuildStageCounters("general"), slotLimit);
            Manager = std::make_shared<NOlap::NGroupedMemoryManager::TManager>(NActors::TActorId(), Config, "test", Counters, Stage);
        }

        void StartProcess(const ui64 processId, const std::vector<ui64>& groups) {
            Manager->RegisterProcess(processId, {});
            Manager->RegisterProcessScope(processId, 0);
            for (auto&& g : groups) {
                Manager->RegisterGroup(processId, 0, g);
            }
        }

        void FinishProcess(const ui64 processId, const std::vector<ui64>& groups) {
            for (auto&& g : groups) {
                Manager->UnregisterGroup(processId, 0, g);
            }
            Manager->UnregisterProcessScope(processId, 0);
            Manager->UnregisterProcess(processId);
        }

        void Free(const ui64 processId, const std::shared_ptr<TAllocation>& alloc) {
            alloc->Guard.reset();
            Manager->UnregisterAllocation(processId, 0, alloc->GetIdentifier());
        }
    };

    Y_UNIT_TEST(SlotsOffKeepsCurrentAdmission) {
        TSlotsEnv env(0, 100, 300, 200);
        UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetSlotLimit(), 200);
        {
            NOlap::NGroupedMemoryManager::TStageFeatures plain("GLOBAL", 100, std::nullopt, nullptr, nullptr);
            UNIT_ASSERT_VALUES_EQUAL(plain.GetSlotLimit(), plain.GetLimit());
        }
        {
            env.StartProcess(0, { 1, 2 });
            env.StartProcess(1, { 1 });
            auto a0 = std::make_shared<TAllocation>(100);
            env.Manager->RegisterAllocation(0, 0, 1, a0, {});
            UNIT_ASSERT(a0->IsAllocated());
            auto a0b = std::make_shared<TAllocation>(100);
            env.Manager->RegisterAllocation(0, 0, 2, a0b, {});
            UNIT_ASSERT(!a0b->IsAllocated());
            auto a1 = std::make_shared<TAllocation>(50);
            env.Manager->RegisterAllocation(1, 0, 1, a1, {});
            UNIT_ASSERT(!a1->IsAllocated());

            env.Free(0, a0);
            UNIT_ASSERT(a0b->IsAllocated());
            UNIT_ASSERT(!a1->IsAllocated());

            env.Free(0, a0b);
            UNIT_ASSERT(a1->IsAllocated());
            env.Free(1, a1);
            env.FinishProcess(1, { 1 });
            env.FinishProcess(0, { 1, 2 });
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(env.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(SlotHolderAdmittedBetweenSoftAndSlotLimit) {
        TSlotsEnv env(2, 100, 300, 200);
        {
            env.StartProcess(0, { 1 });
            env.StartProcess(1, { 1 });
            env.StartProcess(2, { 1 });

            auto a2a = std::make_shared<TAllocation>(30);
            env.Manager->RegisterAllocation(2, 0, 1, a2a, {});
            UNIT_ASSERT(a2a->IsAllocated());

            auto a0 = std::make_shared<TAllocation>(100);
            env.Manager->RegisterAllocation(0, 0, 1, a0, {});
            UNIT_ASSERT(a0->IsAllocated());
            UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 130);

            auto a2b = std::make_shared<TAllocation>(50);
            env.Manager->RegisterAllocation(2, 0, 1, a2b, {});
            UNIT_ASSERT(!a2b->IsAllocated());

            auto a1 = std::make_shared<TAllocation>(50);
            env.Manager->RegisterAllocation(1, 0, 1, a1, {});
            UNIT_ASSERT(a1->IsAllocated());
            UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 180);

            auto a1b = std::make_shared<TAllocation>(60);
            env.Manager->RegisterAllocation(1, 0, 1, a1b, {});
            UNIT_ASSERT(!a1b->IsAllocated());

            env.Free(0, a0);
            UNIT_ASSERT(a1b->IsAllocated());
            UNIT_ASSERT(!a2b->IsAllocated());
            UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 140);

            env.Free(1, a1);
            env.Free(1, a1b);
            UNIT_ASSERT(a2b->IsAllocated());

            env.Free(2, a2a);
            env.Free(2, a2b);
            env.FinishProcess(2, { 1 });
            env.FinishProcess(1, { 1 });
            env.FinishProcess(0, { 1 });
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(env.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(HardLimitStillFailsSlotHolder) {
        TSlotsEnv env(2, 100, 300, 1000);
        UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetSlotLimit(), 300);
        {
            env.StartProcess(0, { 1 });
            env.StartProcess(1, { 1 });

            auto a0 = std::make_shared<TAllocation>(250);
            env.Manager->RegisterAllocation(0, 0, 1, a0, {});
            UNIT_ASSERT(a0->IsAllocated());

            auto a1 = std::make_shared<TAllocation>(40);
            env.Manager->RegisterAllocation(1, 0, 1, a1, {});
            UNIT_ASSERT(a1->IsAllocated());

            auto a1b = std::make_shared<TAllocation>(20);
            env.Manager->RegisterAllocation(1, 0, 1, a1b, {});
            UNIT_ASSERT(!a1b->IsAllocated());

            auto forced = std::make_shared<TFailableAllocation>(100);
            env.Manager->RegisterAllocation(0, 0, 1, forced, {});
            UNIT_ASSERT(forced->Failed);
            UNIT_ASSERT(!forced->IsAllocated());
            UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 290);

            env.Free(0, a0);
            UNIT_ASSERT(a1b->IsAllocated());
            env.Free(1, a1);
            env.Free(1, a1b);
            env.FinishProcess(1, { 1 });
            env.FinishProcess(0, { 1 });
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(env.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(FreedSlotMovesToOldestProcessAndWakesQueued) {
        TSlotsEnv env(2, 100, 300, 200);
        {
            env.StartProcess(0, { 1 });
            env.Manager->RegisterProcess(1, {});
            env.Manager->RegisterProcess(1, {});
            env.StartProcess(2, { 1 });
            env.StartProcess(3, { 1 });

            auto a0 = std::make_shared<TAllocation>(100);
            env.Manager->RegisterAllocation(0, 0, 1, a0, {});
            UNIT_ASSERT(a0->IsAllocated());

            auto a2 = std::make_shared<TAllocation>(50);
            env.Manager->RegisterAllocation(2, 0, 1, a2, {});
            UNIT_ASSERT(!a2->IsAllocated());
            auto a3 = std::make_shared<TAllocation>(50);
            env.Manager->RegisterAllocation(3, 0, 1, a3, {});
            UNIT_ASSERT(!a3->IsAllocated());

            env.Manager->UnregisterProcess(1);
            UNIT_ASSERT(!a2->IsAllocated());

            env.Manager->UnregisterProcess(1);
            UNIT_ASSERT(a2->IsAllocated());
            UNIT_ASSERT(!a3->IsAllocated());
            UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 150);

            env.Free(0, a0);
            UNIT_ASSERT(a3->IsAllocated());
            env.Free(2, a2);
            env.Free(3, a3);
            env.FinishProcess(3, { 1 });
            env.FinishProcess(2, { 1 });
            env.FinishProcess(0, { 1 });
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(env.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }

    Y_UNIT_TEST(RootGateDoesNotStarveQueuedSlotHolder) {
        TSlotsEnv env(2, 100, 300, 200);
        {
            env.StartProcess(0, { 1, 2 });
            env.StartProcess(1, { 1 });
            env.StartProcess(2, { 1 });

            auto a0 = std::make_shared<TAllocation>(100);
            env.Manager->RegisterAllocation(0, 0, 1, a0, {});
            UNIT_ASSERT(a0->IsAllocated());
            auto a0b = std::make_shared<TAllocation>(100);
            env.Manager->RegisterAllocation(0, 0, 2, a0b, {});
            UNIT_ASSERT(a0b->IsAllocated());
            UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 200);

            auto a1 = std::make_shared<TAllocation>(50);
            env.Manager->RegisterAllocation(1, 0, 1, a1, {});
            UNIT_ASSERT(!a1->IsAllocated());
            auto a2 = std::make_shared<TAllocation>(10);
            env.Manager->RegisterAllocation(2, 0, 1, a2, {});
            UNIT_ASSERT(!a2->IsAllocated());

            env.Free(0, a0b);
            UNIT_ASSERT(a1->IsAllocated());
            UNIT_ASSERT(!a2->IsAllocated());
            UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 150);

            env.Free(0, a0);
            UNIT_ASSERT(a2->IsAllocated());
            env.Free(1, a1);
            env.Free(2, a2);
            env.FinishProcess(2, { 1 });
            env.FinishProcess(1, { 1 });
            env.FinishProcess(0, { 1, 2 });
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Stage->GetUsage().Val(), 0);
        UNIT_ASSERT(env.Manager->IsEmpty());
        UNIT_ASSERT_VALUES_EQUAL(TObjectCounter<TAllocation>::ObjectCount(), 0);
    }
};
