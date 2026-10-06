#pragma once

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/tablet.pb.h>
#include <ydb/core/util/tuples.h>

#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NKikimr::NMemory {

// The tablet memory slots of one Local registrar: no atomics and no locks, it lives in the registrar's actor context
class TTabletMemoryHost {
public:
    using TTabletKey = std::pair<ui64, ui32>; // <tablet id, follower id>

    struct TSums {
        ui64 Total = 0; // sum of Used - Reclaimable, the state the tablets cannot give back
        ui64 Elastic = 0; // sum of Reclaimable
        ui64 ElasticDemand = 0; // sum of the elastic demand each tablet would like to hold

        bool operator==(const TSums&) const = default;
    };

    struct TTabletShare {
        TActorId Executor;
        ui64 Bytes = 0;
    };

    // The fields one metrics tick carried; what it left out keeps the value of the tablet's last report
    struct TReportUpdate {
        std::optional<ui64> Used;
        std::optional<ui64> Demand;
        std::optional<ui64> Reclaimable;

        bool IsEmpty() const {
            return !Used && !Demand && !Reclaimable;
        }

        // Picks the memory fields the tablet actually reported out of a metrics tick
        static TReportUpdate FromMetrics(const NKikimrTabletBase::TMetrics& metrics);
    };

    // What one SetReport changed
    struct TSetReportResult {
        bool SumsChanged = false;
        bool NewSlot = false; // the tablet had no slot before this report
    };

    // Merges the update into the tablet's last report
    TSetReportResult SetReport(TTabletKey tablet, TActorId executor, TTabletTypes::EType tabletType, const TReportUpdate& update);

    // Drops the tablet's slot, so its last report leaves the sums; true when it had one
    bool Forget(TTabletKey tablet);

    void Clear();

    const TSums& GetSums() const {
        return Sums;
    }

    size_t GetSlotsCount() const {
        return Slots.size();
    }

    // What the registrar reports for the Tablets kind
    TConsumerReport GetReport() const {
        return {.Used = Sums.Total, .Demand = Sums.Total, .Reclaimable = 0};
    }

    // What the registrar reports for the TabletsElastic kind: all of it is reclaimable
    TConsumerReport GetElasticReport() const {
        return {.Used = Sums.Elastic, .Demand = Max(Sums.ElasticDemand, Sums.Elastic), .Reclaimable = Sums.Elastic};
    }

    // Splits the elastic limit proportionally to Reclaimable and returns the tablets whose share changed
    TVector<TTabletShare> ApplyElasticLimit(ui64 limitBytes);

    // Every tablet that has a slot, for the zone fan-out
    TVector<TActorId> GetExecutors() const;

    // Publishes TabletMemory/<TabletType>/{Used,Demand,Reclaimable} into the group
    void UpdateCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& group) const;

private:
    struct TSlot {
        TActorId Executor;
        TTabletTypes::EType TabletType = TTabletTypes::TypeInvalid;
        TConsumerReport Report;
        ui64 Share = 0;
    };

    static ui64 ElasticDemandOf(const TConsumerReport& report);
    void AddDelta(TTabletTypes::EType tabletType, const TConsumerReport& before, const TConsumerReport& after);

private:
    THashMap<TTabletKey, TSlot> Slots;
    TMap<TTabletTypes::EType, TConsumerReport> PerType;
    TSums Sums;
};

}
