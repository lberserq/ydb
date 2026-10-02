#include "tablet_memory_admission.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NKikimr::NMemory {

namespace {

struct TRequest {
    ui64 Id = 0;
};

// Drives the admission with a zone the test sets and remembers what it let start
struct TFixture {
    EMemoryZone Zone = EMemoryZone::Green;
    TVector<ui64> Started;
    TMemoryAdmission<TRequest> Admission;

    TFixture()
        : Admission([this]() { return Zone; },
                    [this](THolder<TRequest>&& request, bool) { Started.push_back(request->Id); })
    {
    }

    void Admit(ui64 id, ui64 charge) {
        Admission.Admit(id, MakeHolder<TRequest>(TRequest{.Id = id}), charge);
    }
};

}

Y_UNIT_TEST_SUITE(TTabletMemoryAdmission) {

Y_UNIT_TEST(GreenAdmitsEverything) {
    TFixture fixture;
    for (int i = 1; i <= 4; ++i) {
        fixture.Admit(i, 10);
    }
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().RunningBytes, 40u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 0u);
}

Y_UNIT_TEST(YellowForbidsGrowthAboveTheWatermark) {
    TFixture fixture;
    fixture.Admit(1, 100);
    fixture.Admit(2, 100);

    // The watermark is the running bytes seen on entering the zone, so nothing may grow past it
    fixture.Zone = EMemoryZone::Yellow;
    fixture.Admit(3, 10);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 1u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedBytes, 10u);

    // Room freed under the watermark lets the waiting item in
    fixture.Admission.Release(1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().RunningBytes, 110u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 0u);
}

Y_UNIT_TEST(RedRunsOneAtATime) {
    TFixture fixture;
    fixture.Zone = EMemoryZone::Red;
    fixture.Admit(1, 10);
    fixture.Admit(2, 10);
    fixture.Admit(3, 10);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 2u);

    // Still FIFO, one at a time
    fixture.Admission.Release(1);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.back(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().PostponedCount, 1u);

    // A green zone the tablet is told about drains the rest
    fixture.Zone = EMemoryZone::Green;
    fixture.Admission.OnZoneChanged();
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 3u);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Admission.GetStats().HeldBytes(), 20u);
}

Y_UNIT_TEST(EmptyRunSetAlwaysAdmitsTheHead) {
    TFixture fixture;
    fixture.Zone = EMemoryZone::Red;
    fixture.Admit(1, 1_GB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 1u);

    fixture.Zone = EMemoryZone::Yellow;
    fixture.Admission.Release(1);
    fixture.Admit(2, 1_GB);
    UNIT_ASSERT_VALUES_EQUAL(fixture.Started.size(), 2u);
}

Y_UNIT_TEST(NoZoneSourceBehavesAsGreen) {
    TVector<ui64> started;
    TMemoryAdmission<TRequest> admission({}, [&started](THolder<TRequest>&& request, bool) {
        started.push_back(request->Id);
    });
    for (int i = 1; i <= 3; ++i) {
        admission.Admit(i, MakeHolder<TRequest>(TRequest{.Id = static_cast<ui64>(i)}), 1_GB);
    }
    UNIT_ASSERT_VALUES_EQUAL(started.size(), 3u);
}

}

}
