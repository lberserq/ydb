#include "tablet_memory_host.h"

#include <util/generic/hash.h>
#include <util/generic/singleton.h>
#include <util/string/builder.h>
#include <util/system/guard.h>

namespace NKikimr::NMemory {

namespace {

constexpr ui64 NoShare = Max<ui64>();

TConsumerReport Clamped(TConsumerReport report) {
    report.Demand = Max(report.Demand, report.Used);
    report.Reclaimable = Min(report.Reclaimable, report.Used);
    return report;
}

TConsumerReport Delta(const TConsumerReport& now, const TConsumerReport& before) {
    return {.Used = now.Used - before.Used, .Demand = now.Demand - before.Demand, .Reclaimable = now.Reclaimable - before.Reclaimable};
}

// The part a tablet cannot give back: the state and whatever it holds
TConsumerReport NonElasticPart(const TConsumerReport& clamped) {
    const ui64 nonElastic = clamped.Used - clamped.Reclaimable;
    return {.Used = nonElastic, .Demand = nonElastic, .Reclaimable = 0};
}

// The part a tablet can drop: Used is what it holds now, Demand what it would like to hold
TConsumerReport ElasticPart(const TConsumerReport& clamped) {
    const ui64 nonElastic = clamped.Used - clamped.Reclaimable;
    const ui64 demand = Max(clamped.Reclaimable, clamped.Demand - nonElastic);
    return {.Used = clamped.Reclaimable, .Demand = demand, .Reclaimable = clamped.Reclaimable};
}

// Proportional to the elastic demand; a slot that asks for nothing gets nothing
TVector<ui64> SplitLimitByDemand(ui64 limitBytes, const TVector<ui64>& demands) {
    TVector<ui64> shares(demands.size(), 0);
    ui64 totalDemand = 0;
    for (ui64 demand : demands) {
        totalDemand += demand;
    }
    if (!totalDemand) {
        return shares;
    }
    for (size_t index = 0; index < demands.size(); ++index) {
        shares[index] = static_cast<ui64>(static_cast<unsigned __int128>(limitBytes) * demands[index] / totalDemand);
    }
    return shares;
}

}

// Unsigned wraparound makes a negative delta add up correctly
void TTabletMemoryHost::TSum::Add(const TConsumerReport& delta) {
    Used.fetch_add(delta.Used, std::memory_order_relaxed);
    Demand.fetch_add(delta.Demand, std::memory_order_relaxed);
    Reclaimable.fetch_add(delta.Reclaimable, std::memory_order_relaxed);
}

TConsumerReport TTabletMemoryHost::TSum::Load() const {
    return Clamped({
        .Used = Used.load(std::memory_order_relaxed),
        .Demand = Demand.load(std::memory_order_relaxed),
        .Reclaimable = Reclaimable.load(std::memory_order_relaxed),
    });
}

// The last report of one slot; the aggregate holds it weakly to hand out shares of the elastic limit
struct TTabletMemoryHost::TSlotState {
    std::atomic<ui64> Used{0};
    std::atomic<ui64> Demand{0};
    std::atomic<ui64> Reclaimable{0};
    std::atomic<ui64> Share{NoShare};

    TConsumerReport Load() const {
        return Clamped({
            .Used = Used.load(std::memory_order_relaxed),
            .Demand = Demand.load(std::memory_order_relaxed),
            .Reclaimable = Reclaimable.load(std::memory_order_relaxed),
        });
    }

    TConsumerReport Exchange(const TConsumerReport& report) {
        return Clamped({
            .Used = Used.exchange(report.Used, std::memory_order_relaxed),
            .Demand = Demand.exchange(report.Demand, std::memory_order_relaxed),
            .Reclaimable = Reclaimable.exchange(report.Reclaimable, std::memory_order_relaxed),
        });
    }
};

// The host is a process singleton, so the references to its sums and zone stay valid for the aggregate's life
struct TTabletMemoryHost::TTypeAggregate {
    TSum& Total;
    TSum& ElasticTotal;
    std::atomic<EMemoryZone>& NodeZone;
    TSum Sum;

    TAdaptiveLock Lock;
    size_t SlotsCount = 0;
    THashMap<ui64, std::function<void()>> Wakeups;
    THashMap<ui64, std::weak_ptr<TSlotState>> ElasticSlots;

    TTypeAggregate(TSum& total, TSum& elasticTotal, std::atomic<EMemoryZone>& nodeZone)
        : Total(total)
        , ElasticTotal(elasticTotal)
        , NodeZone(nodeZone)
    {
    }

    // Moves the sums from one clamped report of a slot to another
    void Move(const TConsumerReport& before, const TConsumerReport& now) {
        Sum.Add(Delta(now, before));
        Total.Add(Delta(NonElasticPart(now), NonElasticPart(before)));
        ElasticTotal.Add(Delta(ElasticPart(now), ElasticPart(before)));
    }

    std::function<void()> TakeWakeup(ui64 slotId) {
        std::function<void()> wake;
        with_lock (Lock) {
            const auto it = Wakeups.find(slotId);
            if (it != Wakeups.end()) {
                wake = std::move(it->second);
                Wakeups.erase(it);
            }
        }
        return wake;
    }
};

TTabletMemorySlot::TTabletMemorySlot(std::shared_ptr<TTypeAggregate> aggregate, std::shared_ptr<TSlotState> state,
        TTabletTypes::EType tabletType, ui64 tabletId, ui64 slotId)
    : Aggregate(std::move(aggregate))
    , State(std::move(state))
    , TabletType(tabletType)
    , TabletId(tabletId)
    , SlotId(slotId)
{
}

TIntrusivePtr<TTabletMemorySlot> TTabletMemorySlot::Detached() {
    return new TTabletMemorySlot(nullptr, std::make_shared<TSlotState>(), TTabletTypes::TypeInvalid, 0, 0);
}

TTabletMemorySlot::~TTabletMemorySlot() {
    if (!Aggregate) {
        return;
    }
    with_lock (Aggregate->Lock) {
        --Aggregate->SlotsCount;
        Aggregate->Wakeups.erase(SlotId);
        Aggregate->ElasticSlots.erase(SlotId);
    }
    // The last report leaves the sums together with the slot
    Aggregate->Move(State->Load(), {});
}

void TTabletMemorySlot::SetReport(TConsumerReport report) {
    if (!Aggregate) {
        return;
    }
    const TConsumerReport before = State->Exchange(report);
    const TConsumerReport now = Clamped(report);
    Aggregate->Move(before, now);
    // A slot joins the share registry with its first elastic part and stays there for its life
    if (now.Reclaimable && !ElasticRegistered) {
        ElasticRegistered = true;
        with_lock (Aggregate->Lock) {
            Aggregate->ElasticSlots[SlotId] = State;
        }
    }
}

EMemoryZone TTabletMemorySlot::GetZone() const {
    return Aggregate ? Aggregate->NodeZone.load(std::memory_order_relaxed) : EMemoryZone::Green;
}

std::optional<ui64> TTabletMemorySlot::GetShare() const {
    const ui64 share = State->Share.load(std::memory_order_relaxed);
    return share == NoShare ? std::nullopt : std::optional<ui64>(share);
}

void TTabletMemorySlot::RequestWakeup(std::function<void()> wake) {
    if (!Aggregate) {
        return;
    }
    with_lock (Aggregate->Lock) {
        Aggregate->Wakeups[SlotId] = std::move(wake);
    }
}

TTabletMemoryHost& TTabletMemoryHost::Instance() {
    return *Singleton<TTabletMemoryHost>();
}

std::shared_ptr<TTabletMemoryHost::TTypeAggregate> TTabletMemoryHost::FindAggregate(TTabletTypes::EType tabletType) const {
    with_lock (Lock) {
        const auto it = Aggregates.find(tabletType);
        return it == Aggregates.end() ? nullptr : it->second;
    }
}

TVector<std::shared_ptr<TTabletMemoryHost::TTypeAggregate>> TTabletMemoryHost::ListAggregates() const {
    TVector<std::shared_ptr<TTypeAggregate>> aggregates;
    with_lock (Lock) {
        aggregates.reserve(Aggregates.size());
        for (const auto& [tabletType, aggregate] : Aggregates) {
            aggregates.push_back(aggregate);
        }
    }
    return aggregates;
}

TIntrusivePtr<TTabletMemorySlot> TTabletMemoryHost::RegisterConsumer(TTabletTypes::EType tabletType, ui64 tabletId) {
    std::shared_ptr<TTypeAggregate> aggregate;
    with_lock (Lock) {
        auto& entry = Aggregates[tabletType];
        if (!entry) {
            entry = std::make_shared<TTypeAggregate>(Total, ElasticTotal, NodeZone);
        }
        aggregate = entry;
    }
    with_lock (aggregate->Lock) {
        ++aggregate->SlotsCount;
    }
    const ui64 slotId = NextSlotId.fetch_add(1, std::memory_order_relaxed);
    return new TTabletMemorySlot(std::move(aggregate), std::make_shared<TSlotState>(), tabletType, tabletId, slotId);
}

TConsumerReport TTabletMemoryHost::GetTotal() const {
    return Total.Load();
}

TConsumerReport TTabletMemoryHost::GetTotal(TTabletTypes::EType tabletType) const {
    const auto aggregate = FindAggregate(tabletType);
    return aggregate ? aggregate->Sum.Load() : TConsumerReport{};
}

TConsumerReport TTabletMemoryHost::GetElasticTotal() const {
    return ElasticTotal.Load();
}

TElasticBounds TTabletMemoryHost::GetElasticBounds() const {
    return {.Min = 0, .Max = ElasticTotal.Load().Demand};
}

size_t TTabletMemoryHost::GetSlotsCount() const {
    size_t count = 0;
    for (const auto& aggregate : ListAggregates()) {
        with_lock (aggregate->Lock) {
            count += aggregate->SlotsCount;
        }
    }
    return count;
}

size_t TTabletMemoryHost::GetSlotsCount(TTabletTypes::EType tabletType) const {
    const auto aggregate = FindAggregate(tabletType);
    if (!aggregate) {
        return 0;
    }
    with_lock (aggregate->Lock) {
        return aggregate->SlotsCount;
    }
}

void TTabletMemoryHost::SetNodeZone(EMemoryZone zone) {
    if (NodeZone.exchange(zone, std::memory_order_relaxed) == zone) {
        return;
    }
    TVector<std::function<void()>> wakeups;
    for (const auto& aggregate : ListAggregates()) {
        THashMap<ui64, std::function<void()>> requested;
        with_lock (aggregate->Lock) {
            requested.swap(aggregate->Wakeups);
        }
        for (auto& [slotId, wake] : requested) {
            wakeups.push_back(std::move(wake));
        }
    }
    // Called with no lock held: a wake-up may re-enter the host
    for (const auto& wake : wakeups) {
        wake();
    }
}

void TTabletMemoryHost::ApplyElasticLimit(ui64 limitBytes) {
    struct TElasticSlot {
        std::shared_ptr<TTypeAggregate> Aggregate;
        ui64 SlotId;
        std::shared_ptr<TSlotState> State;
    };
    TVector<TElasticSlot> slots;
    TVector<ui64> demands;
    for (const auto& aggregate : ListAggregates()) {
        with_lock (aggregate->Lock) {
            for (const auto& [slotId, weakState] : aggregate->ElasticSlots) {
                if (auto state = weakState.lock()) {
                    demands.push_back(ElasticPart(state->Load()).Demand);
                    slots.push_back({aggregate, slotId, std::move(state)});
                }
            }
        }
    }
    const TVector<ui64> shares = SplitLimitByDemand(limitBytes, demands);
    TVector<std::function<void()>> wakeups;
    for (size_t index = 0; index < slots.size(); ++index) {
        if (slots[index].State->Share.exchange(shares[index], std::memory_order_relaxed) == shares[index]) {
            continue;
        }
        if (auto wake = slots[index].Aggregate->TakeWakeup(slots[index].SlotId)) {
            wakeups.push_back(std::move(wake));
        }
    }
    for (const auto& wake : wakeups) {
        wake();
    }
}

void TTabletMemoryHost::UpdateCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& group) {
    TVector<std::pair<TTabletTypes::EType, TConsumerReport>> sums;
    with_lock (Lock) {
        sums.reserve(Aggregates.size());
        for (const auto& [tabletType, aggregate] : Aggregates) {
            sums.emplace_back(tabletType, aggregate->Sum.Load());
        }
    }
    for (const auto& [tabletType, sum] : sums) {
        const TString prefix = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(tabletType) << "/";
        group->GetCounter(prefix + "Used")->Set(sum.Used);
        group->GetCounter(prefix + "Demand")->Set(sum.Demand);
        group->GetCounter(prefix + "Reclaimable")->Set(sum.Reclaimable);
    }
}

}
