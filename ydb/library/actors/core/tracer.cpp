#include "tracer.h"


#include "actor.h"
#include "actorid.h"

#include <ydb/library/actors/trace_data/trace_data.h>

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>
#include <library/cpp/containers/ring_buffer/ring_buffer.h>
#include <library/cpp/threading/queue/mpsc_htswap.h>

#include <util/system/thread.h>

// TODO REMOVE DEBUG
#include <util/stream/file.h>
// END

namespace NActors::NTracing {

    class TInternalTracer {
    private:
        using TMessage = TEvent;
    public:
        explicit TInternalTracer(TSettings&& settings)
            : Settings(std::move(settings))
        {}

        void AddMessage(TMessage&& message) {
            message.Timestamp = TInstant::Now().Seconds();
            auto& buffer = ThreadId2Buffer.InsertIfAbsentWithInit(TThread::CurrentThreadId(), [&queue=this->Queue, &settings=this->Settings]{
                auto buffer = MakeAtomicShared<TRingBuffer>(settings.MaxBufferSizePerThread);
                auto queueItem = MakeHolder<TRingBufferWithTid>(TRingBufferWithTid{.Buffer = buffer, .ThreadId = TThread::CurrentThreadId()});
                queue.Push(std::move(queueItem));
                return buffer;
            });
            buffer->PushBack(std::move(message));
        }

        void Dump() {
            // DEBUG ONLY
            TFileOutput fout("actors_dump.bin");
            auto buffer = DumpImpl();
            fout.Write(buffer.data(), buffer.Size());
            fout.Flush();
        }
    private:
        TBuffer DumpImpl() {
            TBuffer res;
            while (!Queue.IsEmpty()) {
                auto bufferItem = Queue.Pop();
                if (!bufferItem) {
                    continue;
                }

                {
                    auto bufferFromMap = ThreadId2Buffer.Remove(bufferItem->ThreadId);
                    Y_UNUSED(bufferFromMap);
                }
                auto& buffer = *bufferItem->Buffer;
#define WRITE_TO_BUFF(buff, MEMBER_NAME) \
                buff.Append(reinterpret_cast<const char*>(&item.MEMBER_NAME), sizeof(item.MEMBER_NAME));

                auto writer = [&res](auto&& item) {
                    WRITE_TO_BUFF(res, Timestamp);
                    WRITE_TO_BUFF(res, Type);
                    switch (item.Type) {
                        case TMessage::EType::SendInterconnect:
                            WRITE_TO_BUFF(res, Event.SendInterconnectEvent);
                            break;
                        case TMessage::EType::RecieveInterconnect:
                            WRITE_TO_BUFF(res, Event.RecieveInterconnectEvent);
                            break;
                        case TMessage::EType::SendLocal:
                            WRITE_TO_BUFF(res, Event.SendLocalEvent);
                            break;
                        case TMessage::EType::RecieveLocal:
                            WRITE_TO_BUFF(res, Event.RecieveLocalEvent);
                            break;
                        case TMessage::EType::New:
                            WRITE_TO_BUFF(res, Event.NewEvent);
                            break;
                        case TMessage::EType::Die:
                            WRITE_TO_BUFF(res, Event.DieEvent);
                            break;
                    }
                };
#undef WRITE_TO_BUFF

                for (size_t idx = buffer.FirstIndex(); idx < buffer.TotalSize(); ++idx) {
                    writer(buffer[idx]);
                }
            }
            return res;
        }
    private:
        TSettings Settings;
        using TRingBuffer = TSimpleRingBuffer<TMessage>;
        using TRingBufferPtr = TAtomicSharedPtr<TRingBuffer>;
        static constexpr size_t DEFAULT_BUCKET_COUNT = 64;
        TConcurrentHashMap<TThread::TId, TRingBufferPtr, DEFAULT_BUCKET_COUNT, TSpinLock> ThreadId2Buffer;
        struct TRingBufferWithTid {
            TRingBufferPtr Buffer;
            TThread::TId ThreadId;
        };
        NThreading::THTSwapQueue<THolder<TRingBufferWithTid>> Queue;
    };

    TLocalActorId ConvertToLocalId(const TActorId& actorId) {
        return {.LocalId = actorId.LocalId()};
    }

    TGlobalActorId ConvertToGlobalId(const TActorId& actorId) {
        return {.LocalId = actorId.LocalId(), .NodeId = actorId.NodeId()};
    }

    class TActorTracer: public IActorTracer {
    public:
        explicit TActorTracer(TSettings settings)
            : TracerImpl(std::move(settings))
        {
            Start();
        }

        void HandleNew(IActor& actor) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message {
                .Event = {
                    .NewEvent = {
                        .ActorId = ConvertToLocalId(actor.SelfId())
                    }
                },
                .Type = TEvent::EType::New
            };
            TracerImpl.AddMessage(std::move(message));
        }

        void HandleDie(IActor& actor) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message {
                .Event = {
                    .DieEvent = {
                        .ActorId = ConvertToLocalId(actor.SelfId())
                    }
                },
                .Type = TEvent::EType::Die
            };
            TracerImpl.AddMessage(std::move(message));
        }

        void HandleInterconnectSend(IEventHandle& event, ui32 interconnectSequenceId) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }
            TEvent message = {
                .Event = {
                    .SendInterconnectEvent = {
                        .Sender = ConvertToGlobalId(event.Sender),
                        .Recipient = ConvertToGlobalId(event.Recipient),
                        .ObjectId = static_cast<ui64>(reinterpret_cast<ptrdiff_t>(&event)),
                    }
                },
                .Type = TEvent::EType::SendInterconnect
            };
            message.Event.SendInterconnectEvent.InterconnectSequenceId = interconnectSequenceId;
            TracerImpl.AddMessage(std::move(message));
        }

        void HandleInterconnectRecieve(IEventHandle& event, ui32 interconnectSequenceId) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message = {
                .Event = {
                    .RecieveInterconnectEvent = {
                        .Sender = ConvertToGlobalId(event.Sender),
                        .Recipient = ConvertToGlobalId(event.Recipient),
                        .ObjectId = static_cast<ui64>(reinterpret_cast<ptrdiff_t>(&event)),
                        .MessageType = event.GetTypeRewrite()
                    }
                },
                .Type = TEvent::EType::RecieveInterconnect
            };
            message.Event.RecieveInterconnectEvent.InterconnectSequenceId = interconnectSequenceId;

            TracerImpl.AddMessage(std::move(message));
        }

        void HandleSend(IEventHandle& event) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message = {
                .Event = {
                    .SendLocalEvent = {
                        .Sender = ConvertToLocalId(event.Sender),
                        .Recipient = ConvertToLocalId(event.Recipient),
                        .ObjectId = static_cast<ui64>(reinterpret_cast<ptrdiff_t>(&event))
                    }
                },
                .Type = TEvent::EType::SendLocal
            };
            TracerImpl.AddMessage(std::move(message));
        }

        void HandleReceive(IActor& recipient, IEventHandle& event) override {
            if (!Started.load(std::memory_order::acquire)) {
                return;
            }

            TEvent message = {
                .Event = {
                    .RecieveLocalEvent = {
                        .Sender = ConvertToLocalId(event.Sender),
                        .Recipient = ConvertToLocalId(event.Recipient),
                        .ObjectId = static_cast<ui64>(reinterpret_cast<ptrdiff_t>(&event)),
                        .MessageType = event.GetTypeRewrite()
                    }
                },
                .Type = TEvent::EType::RecieveLocal
            };
            message.Event.RecieveLocalEvent.ActivityIndex = recipient.GetActivityType().GetIndex();

            TracerImpl.AddMessage(std::move(message));
        }

        void Start() override {
            Started.store(true, std::memory_order::release);
        }

        void Stop() override {
            Started.store(false, std::memory_order::release);
        }

        ~TActorTracer() override {
            Stop();
            TracerImpl.Dump();
        }

    private:
        TInternalTracer TracerImpl;
        std::atomic<bool> Started;
    };

    THolder<IActorTracer> CreateActorTracer(TSettings settings) {
       return MakeHolder<TActorTracer>(std::move(settings));
    }
}
