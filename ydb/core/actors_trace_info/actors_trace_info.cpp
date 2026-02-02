#include "actors_trace_info.h"

#include <ydb/core/base/defs.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NActorsTraceInfo {

class TActorsTraceProviderService : public TActorBootstrapped<TActorsTraceProviderService> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MONITORING_SERVICE;
    }

    TActorsTraceProviderService()= default;

    void Bootstrap() {
        Become(&TActorsTraceProviderService::StateWork);
    }

    void Handle(TEvActorsTraceStartRequest::TPtr& ev, const TActorContext& ctx) {
        THolder<TEvActorsTraceStartResponse> response = MakeHolder<TEvActorsTraceStartResponse>();
        ctx.ActorSystem()->GetActorTracer()->Start();
        ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
    }

    void Handle(TEvActorsTraceGetInfoRequest::TPtr& ev, const TActorContext& ctx) {
        THolder<TEvActorsTraceGetInfoResponse> response = MakeHolder<TEvActorsTraceGetInfoResponse>();
        auto& record = response->Record;
        ctx.ActorSystem()->GetActorTracer()->Stop();
        auto data = ctx.ActorSystem()->GetActorTracer()->GetTraceData();
        {
            auto header = NActors::NTracing::SerializeHeader(std::move(data.ActivityDict), std::move(data.EventNamesDict));
            *record.MutableHeader() = std::move(TString{header.Begin(), header.End()});
        }

        {
            auto events = NActors::NTracing::SerializeEvents(std::move(data.Events));
            *record.MutableEvents() = std::move(TString{events.Begin(), events.End()});
        }
        ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvActorsTraceStartRequest, Handle);
            HFunc(TEvActorsTraceGetInfoRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }

private:
};

IActor* CreateActorsTraceProviderService() {
    return new TActorsTraceProviderService();
}
}
