#pragma once

#include "memory_controller_iface.h"
#include "tablet_types.h"

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <functional>
#include <memory>

namespace NKikimr::NMemory {

// Node memory pressure as the tablets see it: Green admits, Yellow forbids growth, Red admits one at a time
enum class EMemoryZone : ui8 {
    Green,
    Yellow,
    Red,
};

// Sent by a tablet's wake-up callable to its executor once the node zone changed
struct TEvMemoryZone : public TEventLocal<TEvMemoryZone, EvTabletMemoryZone> {};

class TTabletMemorySlot;

// Process singleton that sums tablet slots per type and in total and holds the node zone they read
class TTabletMemoryHost {
public:
    static TTabletMemoryHost& Instance();

    TIntrusivePtr<TTabletMemorySlot> RegisterConsumer(TTabletTypes::EType tabletType, ui64 tabletId);

    // Sum of all slots with the read-side clamp applied
    TConsumerReport GetTotal() const;
    TConsumerReport GetTotal(TTabletTypes::EType tabletType) const;

    size_t GetSlotsCount() const;
    size_t GetSlotsCount(TTabletTypes::EType tabletType) const;

    EMemoryZone GetNodeZone() const {
        return NodeZone.load(std::memory_order_relaxed);
    }

    // Publishes the zone to every slot and calls the wake-ups that were requested, only when the zone changed
    void SetNodeZone(EMemoryZone zone);

    // Publishes the per-type sums as TabletMemory/<TabletType>/{Used,Demand} in the given group
    void UpdateCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& group);

private:
    friend class TTabletMemorySlot;

    // Three relaxed atomics with a delta add and a clamped read
    struct TSum {
        std::atomic<ui64> Used{0};
        std::atomic<ui64> Demand{0};
        std::atomic<ui64> Reclaimable{0};

        void Add(const TConsumerReport& delta);
        TConsumerReport Load() const;
    };

    struct TTypeAggregate;

    std::shared_ptr<TTypeAggregate> FindAggregate(TTabletTypes::EType tabletType) const;

private:
    mutable TAdaptiveLock Lock;
    TMap<TTabletTypes::EType, std::shared_ptr<TTypeAggregate>> Aggregates;
    TSum Total;
    std::atomic<EMemoryZone> NodeZone{EMemoryZone::Green};
    std::atomic<ui64> NextSlotId{1};
};

// One tablet's entry in the host: the tablet writes reports and reads the zone through its type aggregate
class TTabletMemorySlot : public TThrRefBase {
    friend class TTabletMemoryHost;
    using TTypeAggregate = TTabletMemoryHost::TTypeAggregate;

public:
    // A slot linked to no host: SetReport is a no-op and the zone stays Green
    static TIntrusivePtr<TTabletMemorySlot> Detached();

    ~TTabletMemorySlot();

    // Three relaxed stores plus the delta into the type and grand sums; only the owning tablet calls it
    void SetReport(TConsumerReport report);

    // Relaxed load, safe on the admission path
    EMemoryZone GetZone() const;

    // One-shot: the next zone change calls the callable and forgets it; a later request replaces it
    void RequestWakeup(std::function<void()> wake);

    bool IsAttached() const {
        return Aggregate != nullptr;
    }

    TTabletTypes::EType GetTabletType() const {
        return TabletType;
    }

    ui64 GetTabletId() const {
        return TabletId;
    }

private:
    TTabletMemorySlot(std::shared_ptr<TTypeAggregate> aggregate, TTabletTypes::EType tabletType, ui64 tabletId, ui64 slotId);

private:
    const std::shared_ptr<TTypeAggregate> Aggregate;
    const TTabletTypes::EType TabletType;
    const ui64 TabletId;
    const ui64 SlotId;

    std::atomic<ui64> Used{0};
    std::atomic<ui64> Demand{0};
    std::atomic<ui64> Reclaimable{0};
};

}
