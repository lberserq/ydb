#include "tablet_memory_host.h"

#include <util/generic/hash.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/guard.h>

namespace NKikimr::NMemory {

// Unsigned wraparound makes a negative delta add up correctly
void TTabletMemoryHost::TSum::Add(const TConsumerReport& delta) {
    Used.fetch_add(delta.Used, std::memory_order_relaxed);
    Demand.fetch_add(delta.Demand, std::memory_order_relaxed);
    Reclaimable.fetch_add(delta.Reclaimable, std::memory_order_relaxed);
}

TConsumerReport TTabletMemoryHost::TSum::Load() const {
    TConsumerReport report{
        .Used = Used.load(std::memory_order_relaxed),
        .Demand = Demand.load(std::memory_order_relaxed),
        .Reclaimable = Reclaimable.load(std::memory_order_relaxed),
    };
    report.Demand = Max(report.Demand, report.Used);
    report.Reclaimable = Min(report.Reclaimable, report.Used);
    return report;
}

// The host is a process singleton, so the references to its sum and zone stay valid for the aggregate's life
struct TTabletMemoryHost::TTypeAggregate {
    TSum& Total;
    std::atomic<EMemoryZone>& NodeZone;
    TSum Sum;

    TAdaptiveLock Lock;
    size_t SlotsCount = 0;
    THashMap<ui64, std::function<void()>> Wakeups;

    TTypeAggregate(TSum& total, std::atomic<EMemoryZone>& nodeZone)
        : Total(total)
        , NodeZone(nodeZone)
    {
    }

    void Add(const TConsumerReport& delta) {
        Sum.Add(delta);
        Total.Add(delta);
    }
};

TTabletMemorySlot::TTabletMemorySlot(std::shared_ptr<TTypeAggregate> aggregate, TTabletTypes::EType tabletType, ui64 tabletId, ui64 slotId)
    : Aggregate(std::move(aggregate))
    , TabletType(tabletType)
    , TabletId(tabletId)
    , SlotId(slotId)
{
}

TIntrusivePtr<TTabletMemorySlot> TTabletMemorySlot::Detached() {
    return new TTabletMemorySlot(nullptr, TTabletTypes::TypeInvalid, 0, 0);
}

TTabletMemorySlot::~TTabletMemorySlot() {
    if (!Aggregate) {
        return;
    }
    with_lock (Aggregate->Lock) {
        --Aggregate->SlotsCount;
        Aggregate->Wakeups.erase(SlotId);
    }
    // The last report leaves the sums together with the slot
    Aggregate->Add({
        .Used = 0 - Used.load(std::memory_order_relaxed),
        .Demand = 0 - Demand.load(std::memory_order_relaxed),
        .Reclaimable = 0 - Reclaimable.load(std::memory_order_relaxed),
    });
}

void TTabletMemorySlot::SetReport(TConsumerReport report) {
    if (!Aggregate) {
        return;
    }
    Aggregate->Add({
        .Used = report.Used - Used.exchange(report.Used, std::memory_order_relaxed),
        .Demand = report.Demand - Demand.exchange(report.Demand, std::memory_order_relaxed),
        .Reclaimable = report.Reclaimable - Reclaimable.exchange(report.Reclaimable, std::memory_order_relaxed),
    });
}

EMemoryZone TTabletMemorySlot::GetZone() const {
    return Aggregate ? Aggregate->NodeZone.load(std::memory_order_relaxed) : EMemoryZone::Green;
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

TIntrusivePtr<TTabletMemorySlot> TTabletMemoryHost::RegisterConsumer(TTabletTypes::EType tabletType, ui64 tabletId) {
    std::shared_ptr<TTypeAggregate> aggregate;
    with_lock (Lock) {
        auto& entry = Aggregates[tabletType];
        if (!entry) {
            entry = std::make_shared<TTypeAggregate>(Total, NodeZone);
        }
        aggregate = entry;
    }
    with_lock (aggregate->Lock) {
        ++aggregate->SlotsCount;
    }
    const ui64 slotId = NextSlotId.fetch_add(1, std::memory_order_relaxed);
    return new TTabletMemorySlot(std::move(aggregate), tabletType, tabletId, slotId);
}

TConsumerReport TTabletMemoryHost::GetTotal() const {
    return Total.Load();
}

TConsumerReport TTabletMemoryHost::GetTotal(TTabletTypes::EType tabletType) const {
    const auto aggregate = FindAggregate(tabletType);
    return aggregate ? aggregate->Sum.Load() : TConsumerReport{};
}

size_t TTabletMemoryHost::GetSlotsCount() const {
    size_t count = 0;
    with_lock (Lock) {
        for (const auto& [tabletType, aggregate] : Aggregates) {
            with_lock (aggregate->Lock) {
                count += aggregate->SlotsCount;
            }
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
    TVector<std::shared_ptr<TTypeAggregate>> aggregates;
    with_lock (Lock) {
        aggregates.reserve(Aggregates.size());
        for (const auto& [tabletType, aggregate] : Aggregates) {
            aggregates.push_back(aggregate);
        }
    }
    TVector<std::function<void()>> wakeups;
    for (const auto& aggregate : aggregates) {
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
    }
}

}
