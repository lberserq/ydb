# -*- coding: utf-8 -*-
import json
import threading
import time
import urllib.request

import pytest
from google.protobuf import text_format

import ydb.public.api.protos.ydb_cms_pb2 as cms_tenants_pb
from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.protobuf_console import AlterTenantRequest, GetTenantStatusRequest
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.fixtures import ydb_database_ctx
from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.stress.common.common import YdbClient
from ydb.tests.stress.olap_workload.workload.type.decommission_ledger import WorkloadDecommissionLedger

_BASELINE_SECS = 30
_AFTER_SECS = 30
_DECOM_TIMEOUT_SECS = 120
_MOVE_QUEUES = ("MoveData/Portions/Pending", "MoveData/Portions/ConfirmedToMove", "MoveData/Portions/InFlight")


def _print_phase_table(phases):
    print("\n{:<20} {:>10} {:>14} {:>10}".format("Phase", "Rows", "Rows/s", "p99 (ms)"))
    print("-" * 60)
    for name, m in phases:
        p99_ms = m["p99_s"] * 1000 if m["p99_s"] == m["p99_s"] else float("nan")
        print("{:<20} {:>10} {:>14.1f} {:>10.1f}".format(name, m["rows"], m["rows_per_s"], p99_ms))


class TestDecommissionE2E(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.NONE,
            extra_feature_flags={
                "enable_move_column_table": True,
                "enable_columnshard_bool": True,
                "enable_cut_history": True,
                "enable_columnshard_group_decommission": True,
            },
            column_shard_config={
                "generate_internal_path_id": True,
                "cut_history_proof_source": "CUT_HISTORY_PROOF_BS_RANGE",
                "cut_history_measure_only": False,
            },
            hive_config={
                "cut_history_deny_list": "KeyValue,PersQueue,BlobDepot",
                "cut_history_allow_list": "DataShard,ColumnShard",
            },
        )

    _move_names_seen = set()

    def _all_nodes(self):
        return list(self.cluster.nodes.values()) + list(self.cluster.slots.values())

    def _tablet_sensors(self):
        """Yield (labels, bare sensor name, value) from every node's tablet counters."""
        for node in self._all_nodes():
            url = "http://localhost:{}/counters/counters=tablets/json".format(node.mon_port)
            try:
                with urllib.request.urlopen(url, timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8", "replace"))
            except Exception:
                continue
            for item in payload.get("sensors", []):
                labels = item.get("labels", {})
                name = labels.get("sensor", "")
                for prefix in ("Deriviative/", "Value/"):
                    if name.startswith(prefix):
                        name = name[len(prefix):]
                        break
                try:
                    yield labels, name, int(item.get("value") or 0)
                except (TypeError, ValueError):
                    continue

    def _cut_history_sensors(self):
        totals = {}
        for labels, name, value in self._tablet_sensors():
            if labels.get("component") == "CutHistory":
                totals[name] = totals.get(name, 0) + value
        return totals

    def _move_data_gauges(self):
        # CS gauges are aggregation clients exported as SUM/, MIN/ and MAX/ series; SUM is the node total.
        totals = {}
        for _, name, value in self._tablet_sensors():
            idx = name.find("SUM/MoveData/")
            if idx >= 0:
                key = name[idx + len("SUM/"):]
                totals[key] = totals.get(key, 0) + value
                self._move_names_seen.add(name)
        return totals

    def _cms_client(self):
        _, _, address = self.endpoint.rpartition("://")
        host, _, port = address.partition(":")
        return kikimr_client_factory(host, port or "2135")

    def _storage_units(self, cms, database):
        request = GetTenantStatusRequest(database)
        response = cms.console_request(text_format.MessageToString(request.protobuf))
        result = cms_tenants_pb.GetDatabaseStatusResult()
        response.GetTenantStatusResponse.Response.operation.result.Unpack(result)
        units = result.required_resources.storage_units
        return units[0] if units else None

    def _alter_units(self, cms, database, delta, unit_kind):
        request = AlterTenantRequest(database)
        if delta < 0:
            request.add_storage_groups_to_remove(unit_kind, -delta)
        else:
            request.add_storage_groups_to_add(unit_kind, delta)
        cms.console_request(text_format.MessageToString(request.protobuf))

    def _wait_units(self, cms, database, expected, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                units = self._storage_units(cms, database)
                if units is not None and units.count == expected:
                    return True
            except Exception:
                pass
            time.sleep(2)
        return False

    def test_decommission_e2e(self):
        with ydb_database_ctx(
            self.cluster, "/Root/decom_e2e", node_count=1, storage_pools={"hdd": 2}
        ) as db_path:
            # Column-store DML is only accepted through QueryService.
            client = YdbClient(self.endpoint, db_path, use_query_service=True)
            client.wait_connection(timeout=30)
            stop = threading.Event()
            ledger = WorkloadDecommissionLedger(client, "e2e", stop)
            try:
                started = ledger.start()
                assert started, "ledger _pre_start failed"

                # Phase 0: baseline throughput before any decommission.
                s0 = ledger.snapshot()
                time.sleep(_BASELINE_SECS)
                s1 = ledger.snapshot()
                baseline = ledger.phase_metrics(s0, s1)

                # Phase 1: shrink pool by one unit, wait for the full pipeline.
                # Full pipeline: BSC decommissions group -> Hive TEvMoveData ->
                # ColumnShard rewrites blobs -> Hive channel reassignment ->
                # CutHistory proves range empty -> cuts history entry.
                # MoveData exports only gauges, so sample them from before the shrink to catch a short move.
                move_seen = {"active": 0, "queued": 0}
                sampling_done = threading.Event()

                def sample_move_data():
                    while not sampling_done.is_set():
                        gauges = self._move_data_gauges()
                        move_seen["active"] = max(move_seen["active"], gauges.get("MoveData/Active", 0))
                        move_seen["queued"] = max(move_seen["queued"], sum(gauges.get(k, 0) for k in _MOVE_QUEUES))
                        time.sleep(1)

                sampler = threading.Thread(target=sample_move_data, daemon=True)
                sampler.start()
                cms = self._cms_client()
                units = self._storage_units(cms, db_path)
                assert units is not None, "tenant reports no storage units"
                unit_kind = units.unit_kind
                initial_count = units.count
                self._alter_units(cms, db_path, -1, unit_kind)
                assert self._wait_units(cms, db_path, initial_count - 1), (
                    "pool did not shrink to {} units".format(initial_count - 1)
                )

                s2 = ledger.snapshot()
                sensors = {}
                deadline = time.time() + _DECOM_TIMEOUT_SECS
                while time.time() < deadline:
                    sensors = self._cut_history_sensors()
                    if sensors.get("Entries/Cut/Count", 0) > 0:
                        break
                    time.sleep(5)
                s3 = ledger.snapshot()
                decom = ledger.phase_metrics(s2, s3)
                sampling_done.set()
                sampler.join(timeout=30)

                # Restore pool so cleanup can converge.
                self._alter_units(cms, db_path, 1, unit_kind)
                self._wait_units(cms, db_path, initial_count, timeout=60)

                # Phase 2: post-decommission; writes must resume at normal throughput.
                s4 = ledger.snapshot()
                time.sleep(_AFTER_SECS)
                s5 = ledger.snapshot()
                after = ledger.phase_metrics(s4, s5)

                stop.set()
                ledger.join(timeout=60)
                errors = ledger.verify()
                _, final_hwm, _ = ledger.snapshot()

                _print_phase_table([("baseline", baseline), ("decommission", decom), ("after", after)])
                print("MoveData peak: active={active} queued portions={queued}".format(**move_seen))
                print("rows verified: {} total, {} written before the decommission".format(
                    sum(final_hwm), sum(s2[1])))
                print("integrity errors: {}".format(len(errors)))
                print("MoveData sensors matched: {}".format(sorted(self._move_names_seen)))
                print("CutHistory: " + ", ".join("{}={}".format(k, sensors.get(k, 0)) for k in (
                    "BootProbe/Nominated/Count", "RangeProbe/Completed/Count", "Entries/Cut/Count",
                    "Channels/Poisoned", "Barriers/Failed/Count")))

                # Each guard proves an event the safety checks below depend on, or they pass vacuously.
                assert sum(s2[1]) > 0, "no rows were written before the decommission, so none were at risk"
                assert move_seen["active"] > 0, "MoveData never started: no TEvMoveData reached a shard"
                assert move_seen["queued"] > 0, "MoveData found nothing in the removed group, so no data was moved"
                assert sensors.get("BootProbe/Nominated/Count", 0) > 0, (
                    "the arm C boot proof never nominated an entry: {}".format(sensors)
                )
                assert sensors.get("RangeProbe/Completed/Count", 0) > 0, (
                    "no TEvRange probe completed, so the cut was not proven by arm C: {}".format(sensors)
                )
                assert not errors, "ledger integrity check failed:\n" + "\n".join(errors[:20])
                assert sensors.get("Entries/Cut/Count", 0) > 0, (
                    "CutHistory full pipeline did not complete: {}".format(sensors)
                )
                assert sensors.get("Channels/Poisoned", 0) == 0, (
                    "CutHistory poisoned a channel (barrier sent to wrong group): {}".format(sensors)
                )
                assert sensors.get("Barriers/Failed/Count", 0) == 0, (
                    "CutHistory barrier send failed: {}".format(sensors)
                )
                # During decommission writes must not crater below 50% of baseline.
                if baseline["rows_per_s"] > 0 and decom["rows"] > 0:
                    ratio = decom["rows_per_s"] / baseline["rows_per_s"]
                    assert ratio >= 0.5, (
                        "decommission throughput {:.1f} r/s is below 50% of baseline {:.1f} r/s".format(
                            decom["rows_per_s"], baseline["rows_per_s"]
                        )
                    )
                # p99 latency must not blow up beyond 3x baseline.
                if (baseline["p99_s"] == baseline["p99_s"] and decom["p99_s"] == decom["p99_s"]
                        and baseline["p99_s"] > 0):
                    ratio = decom["p99_s"] / baseline["p99_s"]
                    assert ratio <= 3.0, (
                        "decommission p99 {:.0f} ms exceeds 3x baseline {:.0f} ms".format(
                            decom["p99_s"] * 1000, baseline["p99_s"] * 1000
                        )
                    )
            finally:
                stop.set()
                ledger.join(timeout=10)
                client.close()
