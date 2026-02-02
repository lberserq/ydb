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

    void Handle(TEvActorsTraceInfoRequest::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvActorsTraceInfoRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }

private:
};

IActor* CreateActorsTraceProviderService() {
    return new TActorsTraceProviderService();
}
}
