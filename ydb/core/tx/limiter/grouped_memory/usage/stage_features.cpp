#include "stage_features.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>

#include <algorithm>
#include <limits>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GROUPED_MEMORY_LIMITER

namespace NKikimr::NOlap::NGroupedMemoryManager {

TString TStageFeatures::DebugString() const {
    TStringBuilder result;
    result << "TStageFeatures{" << Endl
           << "  name=" << Name << Endl
           << "  limit=" << Limit << Endl
           << "  slot_limit=" << SlotLimit << Endl;
    if (Owner) {
        result << "  owner=" << Owner->DebugString() << Endl;
    }
    result << "}";
    return result;
}

ui64 TStageFeatures::ClampSlotLimit(const ui64 slotLimit) const {
    return std::min(std::max(slotLimit, Limit), HardLimit.value_or(std::numeric_limits<ui64>::max()));
}

TStageFeatures::TStageFeatures(const TString& name, const std::optional<ui64>& limit, const std::optional<ui64>& hardLimit,
    const std::shared_ptr<TStageFeatures>& owner, const std::shared_ptr<TStageCounters>& counters, const std::optional<ui64>& slotLimit)
    : Name(name)
    , Limit(limit.value_or(DEFAULT_LIMIT))
    , HardLimit(hardLimit)
    , SlotLimit(ClampSlotLimit(slotLimit.value_or(Limit)))
    , Owner(owner)
    , Counters(counters)
    , UseLimitFromConfig(limit.has_value()) {
    if (Counters) {
        Counters->ValueSoftLimit->Set(Limit);
        Counters->ValueSlotLimit->Set(SlotLimit);
        if (HardLimit) {
            Counters->ValueHardLimit->Set(*HardLimit);
        }
    }
}

TConclusionStatus TStageFeatures::Allocate(const ui64 volume) {
    std::optional<TConclusionStatus> result;
    {
        auto* current = this;
        while (current) {
            current->Waiting.Sub(volume);
            UpdateConsumption(current);
            if (current->Counters) {
                current->Counters->Sub(volume, false);
            }
            if (current->HardLimit && *current->HardLimit < current->Usage.Val() + volume) {
                if (!result) {
                    result = TConclusionStatus::Fail(TStringBuilder() << current->Name << "::(limit:" << *current->HardLimit
                                                                      << ";val:" << current->Usage.Val() << ";delta=" << volume << ");");
                }
                if (current->Counters) {
                    current->Counters->OnCannotAllocate();
                }
                YDB_LOG_DEBUG("",
                    {"name", current->Name},
                    {"event", "cannot_allocate"},
                    {"limit", *current->HardLimit},
                    {"usage", current->Usage.Val()},
                    {"delta", volume});
            }
            current = current->Owner.get();
        }
    }
    if (!!result) {
        return *result;
    }
    {
        auto* current = this;
        while (current) {
            current->Usage.Add(volume);
            UpdateConsumption(current);
            YDB_LOG_DEBUG("",
                {"name", current->Name},
                {"event", "allocate"},
                {"usage", current->Usage.Val()},
                {"delta", volume});
            if (current->Counters) {
                current->Counters->Add(volume, true);
            }
            current = current->Owner.get();
        }
    }
    return TConclusionStatus::Success();
}

void TStageFeatures::Free(const ui64 volume, const bool allocated) {
    auto* current = this;
    while (current) {
        if (current->Counters) {
            current->Counters->Sub(volume, allocated);
        }
        if (allocated) {
            current->Usage.Sub(volume);
        } else {
            current->Waiting.Sub(volume);
        }
        UpdateConsumption(current);
        YDB_LOG_DEBUG("",
            {"name", current->Name},
            {"event", "free"},
            {"usage", current->Usage.Val()},
            {"delta", volume});
        current = current->Owner.get();
    }
}

void TStageFeatures::UpdateVolume(const ui64 from, const ui64 to, const bool allocated) {
    if (Counters) {
        Counters->Sub(from, allocated);
        Counters->Add(to, allocated);
    }
    YDB_LOG_DEBUG("",
        {"name", Name},
        {"event", "update"},
        {"usage", Usage.Val()},
        {"waiting", Waiting.Val()},
        {"allocated", allocated},
        {"from", from},
        {"to", to});
    if (allocated) {
        Usage.Sub(from);
        Usage.Add(to);
    } else {
        Waiting.Sub(from);
        Waiting.Add(to);
    }

    if (Owner) {
        Owner->UpdateVolume(from, to, allocated);
    }

    UpdateConsumption(this);
}

bool TStageFeatures::IsAllocatable(const ui64 volume, const ui64 additional, const bool slotHolder) const {
    // SlotLimit is a root-only threshold; every child stage keeps its own Limit
    const ui64 limit = (slotHolder && !Owner) ? SlotLimit : Limit;
    if (limit < additional + Usage.Val() + volume) {
        return false;
    }
    if (Owner) {
        return Owner->IsAllocatable(volume, additional, slotHolder);
    }
    return true;
}

void TStageFeatures::Add(const ui64 volume, const bool allocated) {
    if (Counters) {
        Counters->Add(volume, allocated);
    }
    if (allocated) {
        Usage.Add(volume);
    } else {
        Waiting.Add(volume);
    }

    if (Owner) {
        Owner->Add(volume, allocated);
    }

    UpdateConsumption(this);
}


void TStageFeatures::SetMemoryConsumptionUpdateFunction(std::function<void(ui64)> func) {
    MemoryConsumptionUpdate = std::move(func);
}

void TStageFeatures::AttachOwner(const std::shared_ptr<TStageFeatures>& owner) {
    if (Owner) {
        return;
    }
    Owner = owner;
}

void TStageFeatures::AttachCounters(const std::shared_ptr<TStageCounters>& counters) {
    if (Counters) {
        return;
    }
    Counters = counters;
    if (Counters) {
        Counters->ValueSoftLimit->Set(Limit);
        Counters->ValueSlotLimit->Set(SlotLimit);
        if (HardLimit) {
            Counters->ValueHardLimit->Set(*HardLimit);
        }
    }
}

void TStageFeatures::UpdateMemoryLimits(const ui64 limit, const std::optional<ui64>& hardLimit, const ui64 slotLimit, bool& isLimitIncreased) {
    if (UseLimitFromConfig) {
        isLimitIncreased = false;
        return;
    }

    Limit = limit;
    HardLimit = hardLimit;
    const ui64 newSlotLimit = ClampSlotLimit(slotLimit);
    isLimitIncreased = limit > Limit || newSlotLimit > SlotLimit;
    SlotLimit = newSlotLimit;

    if (Counters) {
        Counters->ValueSoftLimit->Set(Limit);
        Counters->ValueSlotLimit->Set(SlotLimit);
        if (HardLimit) {
            Counters->ValueHardLimit->Set(*HardLimit);
        }
    }
}

void TStageFeatures::UpdateConsumption(const TStageFeatures* current) const {
    if (!current || !current->MemoryConsumptionUpdate) {
        return;
    }

    current->MemoryConsumptionUpdate(current->Usage.Val());
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
