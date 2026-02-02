#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/actors_trace_info.pb.h>

namespace NKikimr::NActorsTraceInfo {
enum EEv {
    EvActorsTraceStartRequest = EventSpaceBegin(TKikimrEvents::ES_ACTORS_TRACE_INFO),
    EvActorsTraceStartResponse,
    EvActorsTraceGetInfoRequest,
    EvActorsTraceGetInfoResponse,
    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACTORS_TRACE_INFO), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_ACTORS_TRACE_INFO)");

struct TEvActorsTraceStartRequest: public TEventPB<TEvActorsTraceStartRequest, NKikimrActorsTraceInfoProto::TEvActorsTraceStartRequest, EvActorsTraceStartRequest> {};

struct TEvActorsTraceStartResponse: public TEventPB<TEvActorsTraceStartResponse, NKikimrActorsTraceInfoProto::TEvActorsTraceStartResponse, EvActorsTraceStartResponse> {};

struct TEvActorsTraceGetInfoRequest: public TEventPB<TEvActorsTraceGetInfoRequest, NKikimrActorsTraceInfoProto::TEvActorsTraceGetInfoRequest, EvActorsTraceGetInfoRequest> {};

struct TEvActorsTraceGetInfoResponse: public TEventPB<TEvActorsTraceGetInfoResponse, NKikimrActorsTraceInfoProto::TEvActorsTraceGetInfoResponse, EvActorsTraceGetInfoResponse> {};

inline NActors::TActorId MakeActorsTraceProviderServiceID(ui32 nodeId) { return NActors::TActorId(nodeId, "actorTraceInfo"); }
IActor* CreateActorsTraceProviderService();
}
