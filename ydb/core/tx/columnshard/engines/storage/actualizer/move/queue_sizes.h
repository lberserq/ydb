#pragma once

#include <util/system/types.h>

namespace NKikimr::NOlap::NActualizer {

// The gate needs only the total; the split says where a stalled move is stuck.
struct TMoveDataQueueSizes {
    // Driver-owned candidates awaiting metadata classification.
    ui64 Pending = 0;
    ui64 ConfirmedToMove = 0;
    ui64 InFlight = 0;
    // Uncommitted writes with blobs in the target groups: they cannot be rewritten, so the move waits for commit or abort.
    ui64 Uncommitted = 0;
    // Selected target portions not yet erased by cleanup, so their blobs are not queued for GC.
    ui64 Retired = 0;
    // Not a queue: deliberately outside GetTotal(), it explains where a drained queue went.
    ui64 Rejected = 0;

    ui64 GetTotal() const {
        return Pending + ConfirmedToMove + InFlight + Uncommitted;
    }

    TMoveDataQueueSizes& operator+=(const TMoveDataQueueSizes& item) {
        Pending += item.Pending;
        ConfirmedToMove += item.ConfirmedToMove;
        InFlight += item.InFlight;
        Uncommitted += item.Uncommitted;
        Retired += item.Retired;
        Rejected += item.Rejected;
        return *this;
    }
};

}   // namespace NKikimr::NOlap::NActualizer
