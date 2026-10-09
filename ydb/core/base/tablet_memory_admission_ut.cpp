#include "tablet_memory_admission.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>

namespace NKikimr::NMemory {
namespace {

struct TOwner {
    TVector<ui64> Started;

    void StartAdmitted(ui64&& item, EAdmitSource) {
        Started.push_back(item);
    }
};

}

Y_UNIT_TEST_SUITE(TTabletMemoryAdmission) {
    Y_UNIT_TEST(EveryYellowIntervalHasItsOwnWatermark) {
        TOwner owner;
        TMemoryAdmission<ui64, TOwner> admission(owner);
        admission.Admit(1, 1, 100);
        admission.OnZoneChanged(EMemoryZone::Yellow);
        admission.Admit(2, 2, 40);
        admission.Release(1);
        admission.OnZoneChanged(EMemoryZone::Green);
        admission.OnZoneChanged(EMemoryZone::Yellow);
        admission.Admit(3, 3, 60);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().RunningBytes, 40u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().PostponedBytes, 60u);
    }

    Y_UNIT_TEST(GreenDrainsQueuedItemsWithoutACompletion) {
        TOwner owner;
        TMemoryAdmission<ui64, TOwner> admission(owner);
        admission.OnZoneChanged(EMemoryZone::Red);
        admission.Admit(1, 1, 100);
        admission.Admit(2, 2, 40);
        admission.OnZoneChanged(EMemoryZone::Green);
        UNIT_ASSERT_VALUES_EQUAL(owner.Started.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().PostponedCount, 0u);
        // A delivered recovery before enqueue also takes effect immediately.
        admission.Admit(3, 3, 60);
        UNIT_ASSERT_VALUES_EQUAL(owner.Started.size(), 3u);
    }

    Y_UNIT_TEST(CancellingHeadAllowsTheNextFittingItemToStart) {
        TOwner owner;
        TMemoryAdmission<ui64, TOwner> admission(owner);
        admission.Admit(1, 1, 100);
        admission.OnZoneChanged(EMemoryZone::Yellow);
        admission.Admit(2, 2, 40);
        admission.Release(1);
        admission.Admit(3, 3, 100);
        admission.Admit(4, 4, 20);
        auto cancelled = admission.CancelQueued(3);
        UNIT_ASSERT(cancelled);
        UNIT_ASSERT_VALUES_EQUAL(*cancelled, 3u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().RunningBytes, 60u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().RunningCount, 2u);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().PostponedBytes, 0u);
        UNIT_ASSERT_VALUES_EQUAL(owner.Started.back(), 4u);
        UNIT_ASSERT(!admission.CancelQueued(3));
        UNIT_ASSERT(!admission.CancelQueued(2));
        admission.Release(2);
        admission.Release(4);
        UNIT_ASSERT_VALUES_EQUAL(admission.GetStats().HeldBytes(), 0u);
    }
}

}
