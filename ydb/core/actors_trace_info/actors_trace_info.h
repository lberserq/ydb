#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/actors_trace_info.pb.h>

namespace NKikimr::NActorsTraceInfo {
enum EEv {
    EvActorsTraceRequest = EventSpaceBegin(TKikimrEvents::ES_ACTORS_TRACE_INFO),
    EvActorsTraceResponse,
    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACTORS_TRACE_INFO), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACTORS_TRACE_INFO)");

struct TEvActorsTraceInfoRequest : public TEventPB<TEvActorsTraceInfoRequest, NKikimrActorsTraceInfoProto::TEvActorsTraceInfoRequest, EvActorsTraceRequest> {};
struct TEvActorsTraceInfoResponse : public TEventPB<TEvActorsTraceInfoResponse, NKikimrActorsTraceInfoProto::TEvActorsTraceInfoResponse, EvActorsTraceResponse> {};

inline NActors::TActorId MakeActorsTraceProviderServiceID(ui32 nodeId) { return NActors::TActorId(nodeId, "actorTraceInfo"); }
IActor* CreateActorsTraceProviderService();
}
