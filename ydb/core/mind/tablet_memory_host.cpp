#include "tablet_memory_host.h"

#include <util/generic/reserve.h>
#include <util/string/builder.h>

namespace NKikimr::NMemory {

ui64 TTabletMemoryHost::ElasticDemandOf(const TConsumerReport& report) {
    Y_DEBUG_ABORT_UNLESS(report.Reclaimable <= report.Used && report.Demand >= report.Used);
    const ui64 state = report.Used - Min(report.Used, report.Reclaimable);
    return Max(report.Reclaimable, report.Demand - Min(report.Demand, state));
}

void TTabletMemoryHost::AddDelta(TTabletTypes::EType tabletType, const TConsumerReport& before, const TConsumerReport& after) {
    // The row of a type stays once seen, even at zero, so its sensors fall to 0 when its last tablet leaves
    auto& perType = PerType[tabletType];
    perType.Used = perType.Used - before.Used + after.Used;
    perType.Demand = perType.Demand - before.Demand + after.Demand;
    perType.Reclaimable = perType.Reclaimable - before.Reclaimable + after.Reclaimable;

    Sums.Total = Sums.Total - (before.Used - before.Reclaimable) + (after.Used - after.Reclaimable);
    Sums.Elastic = Sums.Elastic - before.Reclaimable + after.Reclaimable;
    Sums.ElasticDemand = Sums.ElasticDemand - ElasticDemandOf(before) + ElasticDemandOf(after);
}

TTabletMemoryHost::TSetReportResult TTabletMemoryHost::SetReport(TTabletKey tablet, TActorId executor,
        TTabletTypes::EType tabletType, const TReportUpdate& update) {
    const auto [it, newSlot] = Slots.try_emplace(tablet);
    auto& slot = it->second;
    const bool executorChanged = !newSlot && slot.Executor != executor;
    if (executorChanged) {
        slot.ShareDelivered = false;
    }
    if (slot.TabletType != tabletType && slot.TabletType != TTabletTypes::TypeInvalid) {
        AddDelta(slot.TabletType, slot.Report, {});
        slot.Report = {};
    }
    slot.Executor = executor;
    slot.TabletType = tabletType;

    TConsumerReport report = slot.Report;
    if (update.Used) {
        report.Used = *update.Used;
    }
    if (update.Demand) {
        report.Demand = *update.Demand;
    }
    if (update.Reclaimable) {
        report.Reclaimable = *update.Reclaimable;
    }
    // A report read off the wire is not trusted to hold the invariants
    report.Demand = Max(report.Demand, report.Used);
    report.Reclaimable = Min(report.Reclaimable, report.Used);

    const bool sharesChanged = newSlot || executorChanged || ElasticDemandOf(slot.Report) != ElasticDemandOf(report);
    const TSums before = Sums;
    AddDelta(tabletType, slot.Report, report);
    slot.Report = report;
    return {.SumsChanged = before != Sums, .NewSlot = newSlot, .ExecutorChanged = executorChanged, .SharesChanged = sharesChanged};
}

bool TTabletMemoryHost::Forget(TTabletKey tablet) {
    const auto it = Slots.find(tablet);
    if (it == Slots.end()) {
        return false;
    }
    const TSums before = Sums;
    AddDelta(it->second.TabletType, it->second.Report, {});
    Slots.erase(it);
    return before != Sums;
}

void TTabletMemoryHost::Clear(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    Slots.clear();
    if (counters) {
        for (auto& [type, report] : PerType) {
            report = {};
        }
        UpdateCounters(counters);
    }
    PerType.clear();
    Sums = {};
}

TVector<TTabletMemoryHost::TTabletShare> TTabletMemoryHost::ApplyElasticLimit(ui64 limitBytes) {
    TVector<TTabletShare> changed;
    for (auto& [tablet, slot] : Slots) {
        const ui64 demand = ElasticDemandOf(slot.Report);
        std::optional<ui64> share;
        if (demand) {
            share = static_cast<ui64>((static_cast<unsigned __int128>(limitBytes) * demand) / Sums.ElasticDemand);
        }
        // Retain the last sent allocation when suppressing growth, so small increases
        // accumulate. Reductions and withdrawals must reach the owner to keep the
        // sum of delivered allocations within the computed budget.
        // Growth is significant at 1 MiB or 1% of the last sent allocation.
        constexpr ui64 SignificantShareIncrease = 1 << 20;
        if (slot.ShareDelivered && slot.Share && *slot.Share && share && *share > *slot.Share &&
            *share - *slot.Share < Min(SignificantShareIncrease, Max<ui64>(1, *slot.Share / 100)))
        {
            continue;
        }
        if (share != slot.Share || (share && !slot.ShareDelivered)) {
            slot.Share = share;
            slot.ShareDelivered = true;
            changed.push_back({.Executor = slot.Executor, .Bytes = share});
        }
    }
    return changed;
}

TVector<TActorId> TTabletMemoryHost::GetExecutors() const {
    TVector<TActorId> result(::Reserve(Slots.size()));
    for (const auto& [tablet, slot] : Slots) {
        result.push_back(slot.Executor);
    }
    return result;
}

void TTabletMemoryHost::UpdateCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& group) const {
    for (const auto& [tabletType, sum] : PerType) {
        const TString prefix = TStringBuilder() << "TabletMemory/" << TTabletTypes::TypeToStr(tabletType) << "/";
        group->GetCounter(prefix + "Used")->Set(sum.Used);
        group->GetCounter(prefix + "Demand")->Set(sum.Demand);
        group->GetCounter(prefix + "Reclaimable")->Set(sum.Reclaimable);
    }
}

}
