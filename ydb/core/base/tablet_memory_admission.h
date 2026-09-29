#pragma once

#include "tablet_memory_host.h"

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>

#include <functional>

namespace NKikimr::NMemory {

struct TMemoryAdmissionStats {
    ui64 RunningBytes = 0;
    ui64 RunningCount = 0;
    ui64 PostponedBytes = 0;
    ui64 PostponedCount = 0;

    ui64 HeldBytes() const {
        return RunningBytes + PostponedBytes;
    }
};

// Gates the start of memory-charged items by the node zone of the tablet's slot: FIFO, no growth in Yellow, one at a time in Red
template <class TItem>
class TMemoryAdmission {
public:
    using TStart = std::function<void(THolder<TItem>&&, bool postponed)>;
    using TWake = std::function<void()>;

    TMemoryAdmission() = default;

    // The wake callable is what the slot calls on the next zone change while items wait
    TMemoryAdmission(TIntrusivePtr<TTabletMemorySlot> slot, TWake wake, TStart start)
        : Slot(std::move(slot))
        , Wake(std::move(wake))
        , Start(std::move(start))
    {
    }

    // Starts the item now or queues it behind the items already waiting
    void Admit(ui64 uid, THolder<TItem>&& item, ui64 charge) {
        if (Queue.empty() && Admits(charge)) {
            Run(uid, std::move(item), charge, /* postponed */ false);
            return;
        }
        PostponedBytes += charge;
        Queue.push_back({uid, charge, std::move(item)});
        Slot->RequestWakeup(Wake);
    }

    // Takes the charge of a completed item back and drains the queue
    void Release(ui64 uid) {
        const auto it = Charges.find(uid);
        if (it == Charges.end()) {
            return;
        }
        Y_DEBUG_ABORT_UNLESS(RunningCount && RunningBytes >= it->second);
        RunningBytes -= it->second;
        --RunningCount;
        Charges.erase(it);
        Drain();
    }

    void OnZoneChanged() {
        Drain();
    }

    TMemoryAdmissionStats GetStats() const {
        return {
            .RunningBytes = RunningBytes,
            .RunningCount = RunningCount,
            .PostponedBytes = PostponedBytes,
            .PostponedCount = Queue.size(),
        };
    }

private:
    struct TEntry {
        ui64 Uid;
        ui64 Charge;
        THolder<TItem> Item;
    };

    bool Admits(ui64 charge) {
        const EMemoryZone zone = Slot->GetZone();
        if (zone != LastZone) {
            LastZone = zone;
            Watermark = RunningBytes;
        }
        switch (zone) {
            case EMemoryZone::Green:
                return true;
            case EMemoryZone::Yellow:
                return RunningCount == 0 || RunningBytes + charge <= Watermark;
            case EMemoryZone::Red:
                return RunningCount == 0;
        }
        return true;
    }

    void Run(ui64 uid, THolder<TItem>&& item, ui64 charge, bool postponed) {
        RunningBytes += charge;
        ++RunningCount;
        Charges[uid] = charge;
        Start(std::move(item), postponed);
    }

    void Drain() {
        if (Draining) {
            return;
        }
        Draining = true;
        while (!Queue.empty() && Admits(Queue.front().Charge)) {
            TEntry entry = std::move(Queue.front());
            Queue.pop_front();
            PostponedBytes -= entry.Charge;
            Run(entry.Uid, std::move(entry.Item), entry.Charge, /* postponed */ true);
        }
        Draining = false;
        if (!Queue.empty()) {
            Slot->RequestWakeup(Wake);
        }
    }

private:
    TIntrusivePtr<TTabletMemorySlot> Slot = TTabletMemorySlot::Detached();
    TWake Wake;
    TStart Start;

    EMemoryZone LastZone = EMemoryZone::Green;
    ui64 Watermark = 0;
    ui64 RunningBytes = 0;
    ui64 RunningCount = 0;
    ui64 PostponedBytes = 0;
    THashMap<ui64, ui64> Charges;
    TDeque<TEntry> Queue;
    bool Draining = false;
};

}
