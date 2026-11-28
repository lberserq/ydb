#include "topic_mirroring_fixture.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {
namespace {

Y_UNIT_TEST_SUITE(TopicMirroringFunctional) {
    Y_UNIT_TEST(MixedTopicAndTableReplicationAttachesBothTargets) {
        TTopicMirroringFixture test("localhost", false, nullptr, {}, TTopicMirroringFixture::EStart::Deferred);
        test.CreateSourceTable();
        test.CreateTopicAndTableReplication();

        test.WriteAndReadBack("topic-message", 0);
        test.WaitForOffsets(1, 0);
        test.UpsertSourceTable(1, "table-row");
        UNIT_ASSERT_VALUES_EQUAL(test.GetSourceTableRows(), 1);
        test.CheckTableTargetAttached();
    }

    Y_UNIT_TEST(MixedTopicAndTableTransactionKeepsClustersAlive) {
        using namespace NYdb::NTopic;
        TTopicMirroringFixture test("localhost", false, nullptr, {}, TTopicMirroringFixture::EStart::Deferred);
        test.CreateSourceTable();
        test.CreateTopicAndTableReplication();

        NYdb::NTable::TTableClient tableClient(test.Source.GetDriver());
        auto session = test.Source.RunCall([&] { return tableClient.CreateSession().GetValueSync().GetSession(); });
        auto tx = test.Source.RunCall([&] {
            return session.BeginTransaction(NYdb::NTable::TTxSettings::SerializableRW()).GetValueSync().GetTransaction();
        });
        const auto upsert = test.Source.RunCall([&] {
            return session.ExecuteDataQuery("UPSERT INTO `/Root/src/table` (Key, Value) VALUES (1u, \"tx-row\");",
                NYdb::NTable::TTxControl::Tx(tx)).GetValueSync();
        });
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        TTopicClient topicClient(test.Source.GetDriver());
        auto writer = topicClient.CreateWriteSession(TWriteSessionSettings()
            .Path(test.SourceTopic).ProducerId("tx-producer").MessageGroupId("tx-producer").Codec(ECodec::RAW));
        std::optional<TContinuationToken> token;
        test.Wait([&] {
            if (auto event = writer->GetEvent()) {
                if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
                    token = std::move(ready->ContinuationToken);
                }
            }
            return token.has_value();
        }, "write session ready to accept");
        auto txMessage = TWriteMessage("tx-message");
        txMessage.Tx(tx);
        writer->Write(std::move(*token), std::move(txMessage));

        const auto commit = test.Source.RunCall([&] { return tx.Commit().GetValueSync(); });
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());

        // A transaction spanning the replicated topic and table is unsupported by
        // replication: neither cluster may crash, the topic pipeline must keep
        // delivering, and the table target must stay attached. Delivery of the
        // transactional payload itself is not asserted.
        const auto endBefore = test.GetDestinationEndOffset();
        test.Write({"liveness"});
        test.Wait([&] { return test.GetDestinationEndOffset() > endBefore; },
            "mirror delivery after mixed transaction");
        UNIT_ASSERT_VALUES_EQUAL(test.GetSourceTableRows(), 1);
        test.CheckTableTargetAttached();
    }

    Y_UNIT_TEST(MirrorsMillionMessagesWithTableTargetAttached) {
        TTopicMirroringFixture test("localhost", false, nullptr, {}, TTopicMirroringFixture::EStart::Deferred);
        test.CreateSourceTable();

        constexpr ui64 rowCount = 1'000'000;
        constexpr ui64 batchSize = 10'000;
        for (ui64 begin = 0; begin < rowCount; begin += batchSize) {
            test.BulkUpsertSourceTable(begin, batchSize);
        }
        constexpr size_t messageCount = 1'000'000;
        test.WriteMany(messageCount, "bulk");

        test.CreateTopicAndTableReplication();
        test.Wait([&] { return test.GetDestinationEndOffset() == messageCount; }, "million messages mirrored");
        test.ReadBack({"bulk-999998", "bulk-999999"}, messageCount - 2);
        UNIT_ASSERT_VALUES_EQUAL(test.GetSourceTableRows(), rowCount);
        test.CheckTableTargetAttached();
    }

    Y_UNIT_TEST(MirroringFollowsSourcePartitionSplit) {
        TTopicMirroringFixture test("localhost", false, nullptr, {}, TTopicMirroringFixture::EStart::Deferred);
        test.AlterSourcePartitioning(1, 4, NYdb::NTopic::EAutoPartitioningStrategy::ScaleUp);
        test.Start("localhost");
        test.WriteAndReadBack("before-split", 0);

        test.AlterSourcePartitioning(2, 4, NYdb::NTopic::EAutoPartitioningStrategy::ScaleUp);
        test.WaitActivePartitionCount(test.Source, test.SourceTopic, 2);
        test.WaitActivePartitionCount(test.Destination, test.DestinationTopic, 2);

        test.Write({"child-1"}, 1);
        test.Write({"child-2"}, 2);
        test.WaitForOffsets(1, 0, 1);
        test.WaitForOffsets(1, 0, 2);
        test.ReadBack({"child-1"}, 0, 1);
        test.ReadBack({"child-2"}, 0, 2);
    }
}
}
}
