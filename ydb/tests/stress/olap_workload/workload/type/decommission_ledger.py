# -*- coding: utf-8 -*-
import logging
import threading
import time

from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger(__name__)

_MASK31 = (1 << 31) - 1


class WorkloadDecommissionLedger(WorkloadBase):
    """Append-only column-store ledger; verify() asserts no rows lost after a decommission."""

    def __init__(self, client, prefix, stop, num_shards=4, batch=200):
        super().__init__(client, prefix, "decom_ledger", stop)
        self.num_shards = num_shards
        self.batch = batch
        self._hwm = [0] * num_shards
        self._lock = threading.Lock()
        # Each entry: (wall-clock timestamp, write elapsed seconds).
        self._latencies = []
        self._inserts = 0

    def get_stat(self):
        with self._lock:
            return "inserts={} hwm={}".format(self._inserts, self._hwm)

    def table_path(self):
        return self.get_table_path("ledger")

    def _pre_start(self):
        self.client.query(
            "CREATE TABLE `{}` "
            "(shard_id Uint32 NOT NULL, key Uint64 NOT NULL, val Uint64 NOT NULL, "
            "PRIMARY KEY(shard_id, key)) "
            "WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)".format(
                self.table_path()
            ),
            is_ddl=True,
        )
        return True

    @staticmethod
    def _val(shard_id, key):
        # Stays in Int64 range so plain integer literals work in YQL without a type suffix.
        return (key * 1000003 + shard_id * 999983) & _MASK31

    def _writer(self, shard_id):
        path = self.table_path()
        key = 1
        while not self.is_stop_requested():
            top = key + self.batch
            values = ", ".join(
                "({}, {}, {})".format(shard_id, k, self._val(shard_id, k))
                for k in range(key, top)
            )
            t0 = time.monotonic()
            try:
                self.client.query(
                    "UPSERT INTO `{}` (shard_id, key, val) VALUES {}".format(path, values),
                    is_ddl=False,
                )
                elapsed = time.monotonic() - t0
                with self._lock:
                    self._hwm[shard_id] = top - 1
                    self._inserts += self.batch
                    self._latencies.append((time.time(), elapsed))
                key = top
            except Exception as e:
                logger.warning("decom_ledger shard %d: %s", shard_id, e)
                time.sleep(1)

    def get_workload_thread_funcs(self):
        return [lambda i=i: self._writer(i) for i in range(self.num_shards)]

    def snapshot(self):
        """Return (ts, hwm_list, latency_list) captured atomically."""
        with self._lock:
            return time.time(), list(self._hwm), list(self._latencies)

    def phase_metrics(self, before, after):
        """rows/s and p99 write latency for the interval bracketed by two snapshots."""
        ts_b, hwm_b, _ = before
        ts_a, hwm_a, lats_a = after
        elapsed = max(ts_a - ts_b, 1e-3)
        rows = sum(max(hwm_a[i] - hwm_b[i], 0) for i in range(self.num_shards))
        phase_lats = sorted(lat for ts, lat in lats_a if ts_b <= ts <= ts_a)
        p99 = phase_lats[int(len(phase_lats) * 0.99)] if phase_lats else float("nan")
        return {"rows": rows, "rows_per_s": rows / elapsed, "p99_s": p99}

    def verify(self):
        """Scan every committed shard and return a list of error strings (empty means OK)."""
        _, hwm, _ = self.snapshot()
        path = self.table_path()
        errors = []
        for shard_id, max_key in enumerate(hwm):
            if max_key == 0:
                continue
            result = self.client.query(
                "SELECT COUNT(*) AS cnt FROM `{}` WHERE shard_id = {} AND key <= {}".format(
                    path, shard_id, max_key
                ),
                is_ddl=False,
            )
            cnt = result[0].rows[0]["cnt"]
            if cnt != max_key:
                errors.append("shard={} lost rows: expected {} got {}".format(shard_id, max_key, cnt))
                continue
            result = self.client.query(
                "SELECT val FROM `{}` WHERE shard_id = {} AND key = {}".format(path, shard_id, max_key),
                is_ddl=False,
            )
            rows = result[0].rows
            if not rows:
                errors.append("shard={} last key {} vanished on spot-check".format(shard_id, max_key))
            elif rows[0]["val"] != self._val(shard_id, max_key):
                errors.append(
                    "shard={} key={} val={} expected={}".format(
                        shard_id, max_key, rows[0]["val"], self._val(shard_id, max_key)
                    )
                )
        return errors
