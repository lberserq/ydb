#include <ydb/core/tablet_flat/test/libs/table/test_dbase.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NColumnShard {
namespace {

// Persisted layout from PR #53649, commit e1d8ee2679cde6d83881295833237dcfbc9e1f8e.
// Keep this fixture independent of the current schema to catch incompatible upgrades.
struct TLegacySchema: NIceDb::Schema {
    struct CutHistoryRequests: Table<20> {
        struct Sequence: Column<1, NScheme::NTypeIds::Uint64> {};

        struct TabletID: Column<2, NScheme::NTypeIds::Uint64> {};

        struct Channel: Column<3, NScheme::NTypeIds::Uint32> {};

        struct FromGeneration: Column<4, NScheme::NTypeIds::Uint32> {};

        struct GroupID: Column<5, NScheme::NTypeIds::Uint32> {};

        struct TimestampUs: Column<6, NScheme::NTypeIds::Uint64> {};

        struct Recipient: Column<7, NScheme::NTypeIds::ActorId> {};

        struct ToGeneration: Column<8, NScheme::NTypeIds::Uint32> {};

        struct SendingGeneration: Column<9, NScheme::NTypeIds::Uint32> {};

        struct RequestProto: Column<10, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Sequence>;
        using TColumns = TableColumns<Sequence, TabletID, Channel, FromGeneration, GroupID, TimestampUs, Recipient, ToGeneration,
            SendingGeneration, RequestProto>;
    };

    using TTables = SchemaTables<CutHistoryRequests>;
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardCutHistorySchema) {
    Y_UNIT_TEST(UpgradePreservesLegacyJournal) {
        using TOld = TLegacySchema::CutHistoryRequests;
        using TNew = Schema::CutHistoryRequests;
        NTable::NTest::TDbExec database;
        NKikimrTxColumnShard::TCutHistoryRequest request;
        request.SetTabletID(123);
        request.SetChannel(2);
        request.SetFromGeneration(3);
        request.SetToGeneration(4);
        request.SetGroupID(2181038080);
        const TString oldPayload = request.SerializeAsString();

        database.Begin();
        {
            NIceDb::TNiceDb db(*database.operator->());
            db.Materialize<TLegacySchema>();
            db.Table<TOld>().Key(42).Update(
                NIceDb::TUpdate<TOld::TabletID>(123), NIceDb::TUpdate<TOld::Channel>(2), NIceDb::TUpdate<TOld::RequestProto>(oldPayload));
        }
        database.Commit().Replay(NTable::NTest::EPlay::Boot);

        database.Begin();
        {
            NIceDb::TNiceDb db(*database.operator->());
            // Exercise the same materialization as TTxInitSchema on an existing tablet.
            db.Materialize<Schema>();
            auto row = db.Table<TNew>().Key(42).Select<TNew::RequestProto>();
            UNIT_ASSERT(row.IsReady());
            UNIT_ASSERT(!row.EndOfSet());
            UNIT_ASSERT_VALUES_EQUAL(row.GetValue<TNew::RequestProto>(), oldPayload);

            request.SetFromGeneration(4);
            request.SetToGeneration(5);
            db.Table<TNew>().Key(43).Update(NIceDb::TUpdate<TNew::RequestProto>(request.SerializeAsString()));
        }
        database.Commit().Replay(NTable::NTest::EPlay::Boot);

        database.Begin();
        {
            NIceDb::TNiceDb db(*database.operator->());
            db.Materialize<Schema>();
            auto oldRow = db.Table<TOld>().Key(42).Select<TOld::TabletID, TOld::Channel, TOld::RequestProto>();
            UNIT_ASSERT(oldRow.IsReady());
            UNIT_ASSERT(!oldRow.EndOfSet());
            UNIT_ASSERT_VALUES_EQUAL(oldRow.GetValue<TOld::TabletID>(), 123u);
            UNIT_ASSERT_VALUES_EQUAL(oldRow.GetValue<TOld::Channel>(), 2u);
            UNIT_ASSERT_VALUES_EQUAL(oldRow.GetValue<TOld::RequestProto>(), oldPayload);

            auto newRow = db.Table<TNew>().Key(43).Select<TNew::RequestProto>();
            UNIT_ASSERT(newRow.IsReady());
            UNIT_ASSERT(!newRow.EndOfSet());
            UNIT_ASSERT_VALUES_EQUAL(newRow.GetValue<TNew::RequestProto>(), request.SerializeAsString());

            // The previous binary can still open the table after new journal writes.
            db.Materialize<TLegacySchema>();
            auto rollbackRow = db.Table<TOld>().Key(43).Select<TOld::RequestProto>();
            UNIT_ASSERT(rollbackRow.IsReady());
            UNIT_ASSERT(!rollbackRow.EndOfSet());
            UNIT_ASSERT_VALUES_EQUAL(rollbackRow.GetValue<TOld::RequestProto>(), request.SerializeAsString());
        }
        database.Commit();
    }
}

}   // namespace NKikimr::NColumnShard
