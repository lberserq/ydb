UNITTEST()

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    unused_executor_channels_ut.cpp
)

PEERDIR(
    ydb/core/statistics/aggregator
    ydb/core/sys_view/processor
    ydb/core/testlib/default
    ydb/core/tx/mediator
    ydb/core/tx/schemeshard
)

END()
