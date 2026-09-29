#pragma once

#include "memory_controller_iface.h"
#include "tablet_types.h"

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <functional>
#include <memory>
#include <optional>

namespace NKikimr::NMemory {

// Node memory pressure as the tablets see it: Green admits, Yellow forbids growth, Red admits one at a time
enum class EMemoryZone : ui8 {
    Green,
    Yellow,
    Red,
};

// Sent by a tablet's wake-up callable to its executor once the node zone or the tablet's share changed
struct TEvMemoryZone : public TEventLocal<TEvMemoryZone, EvTabletMemoryZone> {};

// What the elastic parts of the tablets can take: Min is nothing, Max is their summed elastic demand
struct TElasticBounds {
    ui64 Min = 0;
    ui64 Max = 0;
};

class TTabletMemorySlot;

// Process singleton that sums tablet slots per type and by part and holds the node zone they read
class TTabletMemoryHost {
public:
    static TTabletMemoryHost& Instance();

    TIntrusivePtr<TTabletMemorySlot> RegisterConsumer(TTabletTypes::EType tabletType, ui64 tabletId);

    // Sum of the non-elastic parts, Used - Reclaimable of every slot
    TConsumerReport GetTotal() const;
    // Sum of all reports of the type with the read-side clamp applied
    TConsumerReport GetTotal(TTabletTypes::EType tabletType) const;

    // Sum of the elastic parts: Used is the summed Reclaimable, Demand the summed elastic demand
    TConsumerReport GetElasticTotal() const;
    TElasticBounds GetElasticBounds() const;

    size_t GetSlotsCount() const;
    size_t GetSlotsCount(TTabletTypes::EType tabletType) const;

    EMemoryZone GetNodeZone() const {
        return NodeZone.load(std::memory_order_relaxed);
    }

    // Publishes the zone to every slot and calls the wake-ups that were requested, only when the zone changed
    void SetNodeZone(EMemoryZone zone);

    // Splits the limit among the slots with an elastic part by their elastic demand and wakes those whose share changed and who asked
    void ApplyElasticLimit(ui64 limitBytes);

    // Publishes the per-type sums as TabletMemory/<TabletType>/{Used,Demand,Reclaimable} in the given group
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

    struct TSlotState;
    struct TTypeAggregate;

    std::shared_ptr<TTypeAggregate> FindAggregate(TTabletTypes::EType tabletType) const;
    TVector<std::shared_ptr<TTypeAggregate>> ListAggregates() const;

private:
    mutable TAdaptiveLock Lock;
    TMap<TTabletTypes::EType, std::shared_ptr<TTypeAggregate>> Aggregates;
    TSum Total;
    TSum ElasticTotal;
    std::atomic<EMemoryZone> NodeZone{EMemoryZone::Green};
    std::atomic<ui64> NextSlotId{1};
};

// One tablet's entry in the host: the tablet writes reports and reads the zone and its share through it
class TTabletMemorySlot : public TThrRefBase {
    friend class TTabletMemoryHost;
    using TSlotState = TTabletMemoryHost::TSlotState;
    using TTypeAggregate = TTabletMemoryHost::TTypeAggregate;

public:
    // A slot linked to no host: SetReport is a no-op, the zone stays Green and no share ever arrives
    static TIntrusivePtr<TTabletMemorySlot> Detached();

    ~TTabletMemorySlot();

    // Relaxed stores plus the deltas of both parts into the sums; only the owning tablet calls it
    void SetReport(TConsumerReport report);

    // Relaxed load, safe on the admission path
    EMemoryZone GetZone() const;

    // How much elastic part this tablet may keep; nullopt until the first ApplyElasticLimit that saw it
    std::optional<ui64> GetShare() const;

    // One-shot: the next zone or share change calls the callable and forgets it; a later request replaces it
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
    TTabletMemorySlot(std::shared_ptr<TTypeAggregate> aggregate, std::shared_ptr<TSlotState> state,
        TTabletTypes::EType tabletType, ui64 tabletId, ui64 slotId);

private:
    const std::shared_ptr<TTypeAggregate> Aggregate;
    const std::shared_ptr<TSlotState> State;
    const TTabletTypes::EType TabletType;
    const ui64 TabletId;
    const ui64 SlotId;
    bool ElasticRegistered = false;
};

}
