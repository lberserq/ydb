#pragma once
#include <ydb/library/accessor/accessor.h>

#include <optional>

namespace NKikimrConfig {
    class TGroupedMemoryLimiterConfig;
}

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TConfig {
private:
    YDB_READONLY(bool, Enabled, true);
    YDB_READONLY_DEF(std::optional<ui64>, MemoryLimit);
    YDB_READONLY_DEF(std::optional<ui64>, HardMemoryLimit);
    YDB_READONLY(ui64, CountBuckets, 1);
    YDB_READONLY(ui32, SlotsCount, 0);
    YDB_READONLY(double, SlotLimitCoefficient, 0);

public:

    static TConfig BuildDisabledConfig() {
        TConfig result;
        result.Enabled = false;
        return result;
    }

    TConfig WithoutSlots() const {
        TConfig result = *this;
        result.SlotsCount = 0;
        result.SlotLimitCoefficient = 0;
        return result;
    }

    bool IsEnabled() const {
        return Enabled;
    }
    bool DeserializeFromProto(const NKikimrConfig::TGroupedMemoryLimiterConfig& config);
    TString DebugString() const;
};

}
