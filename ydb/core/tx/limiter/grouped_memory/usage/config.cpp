#include "config.h"
#include <util/string/builder.h>

#include <ydb/core/protos/config.pb.h>

#include <algorithm>

namespace NKikimr::NOlap::NGroupedMemoryManager {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TGroupedMemoryLimiterConfig& config) {
    CountBuckets = config.GetCountBuckets() ? config.GetCountBuckets() : 1;

    if (config.HasMemoryLimit()) {
        MemoryLimit = config.GetMemoryLimit() / CountBuckets;
    }

    if (config.HasHardMemoryLimit()) {
        HardMemoryLimit = config.GetHardMemoryLimit() / CountBuckets;
    }

    Enabled = config.GetEnabled();
    SlotsCount = config.GetSlotsCount();
    SlotLimitCoefficient = std::clamp(config.GetSlotLimitCoefficient(), 0.0, 1.0);

    return true;
}

TString TConfig::DebugString() const {
    TStringBuilder sb;
    sb << "MemoryLimit=" << MemoryLimit.value_or(0)
       << ";HardMemoryLimit=" << HardMemoryLimit.value_or(0)
       << ";Enabled=" << Enabled
       << ";CountBuckets=" << CountBuckets
       << ";SlotsCount=" << SlotsCount
       << ";SlotLimitCoefficient=" << SlotLimitCoefficient
       << ";";
    return sb;
}

}
