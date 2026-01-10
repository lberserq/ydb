#pragma once

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>

namespace NActors {
    class IActor;
    class IEventHandle;
    namespace NTracing {
        struct TSettings {
            size_t MaxBufferSizePerThread = 1024;
        };

        class IActorTracer: TNonCopyable {
        public:
            virtual ~IActorTracer() = default;
            virtual void HandleNew(IActor& actor) = 0;
            virtual void HandleDie(IActor& actor) = 0;
            virtual void HandleSend(IEventHandle& event) = 0;
            virtual void HandleReceive(IActor& recipient, IEventHandle& event) = 0;
            virtual void HandleInterconnectSend(IEventHandle& event, ui32 interconnectSequenceId) = 0;
            virtual void HandleInterconnectRecieve(IEventHandle& event, ui32 interconnectSequenceId) = 0;
            virtual void Start() = 0;
            virtual void Stop() = 0;
        };

        THolder<IActorTracer> CreateActorTracer(TSettings);
    }
}
