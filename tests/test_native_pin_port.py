# SPDX-License-Identifier: MIT OR Apache-2.0
from contextlib import contextmanager
import unittest

from ashlar.native import SQLResult
from ashlar.pins import PinVector, PostgresPins
from ashlar_host.native_pin_port import NativePinPort, NativePinPortError


class _Executor:
    def __init__(self):
        self.rows = []
        self.open = False
        self.calls = []

    @contextmanager
    def transaction(self, _context):
        self.open = True
        try:
            yield self
        finally:
            self.open = False

    def query(self, sql, params):
        self.calls.append((sql, dict(params)))
        if "assert_uuid_available" in sql:
            return SQLResult([])
        if "ashlar_pins.register(" in sql:
            self.rows.append({"table_name": params["table"], "table_uuid": params["uuid"],
                              "version": params["version"], "digest": params["digest"],
                              "released": False})
        elif "assert_active(" in sql:
            if not any(row["table_name"] == params["table"] and not row["released"]
                       for row in self.rows):
                raise ValueError("pin missing")
        else:
            return SQLResult([dict(row) for row in self.rows])
        return SQLResult([])


class _Policy:
    def admit_registration(self, _vector, _context):
        return None

    authorize_read = admit_registration

    def authorize_source_bound_read(self, _vector, _source, _context):
        return None


class _Retention:
    def __init__(self):
        self.events = []
        self.remaining = 60
        self.failure = None

    def remaining_lifetime_seconds(self, vector, source, context):
        self.events.append((vector, source, context))
        if self.failure:
            raise self.failure
        return self.remaining


class NativePinPortTests(unittest.TestCase):
    def setUp(self):
        self.executor = _Executor()
        self.pins = PostgresPins(self.executor, _Policy())
        self.vector = PinVector("authority", "manifest", "scope", "a" * 64,
                                {"c.s.object_current": ("uuid-1", 4),
                                 "c.s.edge_current": ("uuid-2", 9),
                                 "c.s.tombstone": ("uuid-3", 2),
                                 "c.s.whole_source_history": ("uuid-4", 7)})
        self.context = object()
        self.source = object()
        self.pins.register(self.vector, context=self.context)
        self.retention = _Retention()
        self.port = NativePinPort(self.pins, self.retention)

    def test_open_renew_and_close_recheck_exact_source_and_complete_pin_vector(self):
        handle = self.port.open(self.vector, self.source, self.context)
        self.assertTrue(self.executor.open)
        self.assertEqual(handle.remaining_lifetime_seconds(), 60.0)
        handle.renew()
        self.assertEqual(self.retention.events, [(self.vector, self.source, self.context)] * 3)
        self.assertEqual(sum("assert_active(" in sql for sql, _ in self.executor.calls), 8)
        handle.close()
        self.assertFalse(self.executor.open)
        self.assertEqual(len(self.retention.events), 4)
        self.assertEqual(sum("assert_active(" in sql for sql, _ in self.executor.calls), 12)
        with self.assertRaisesRegex(NativePinPortError, "closed"):
            handle.renew()

    def test_missing_or_invalid_independent_retention_refuses_and_releases_pin(self):
        self.retention.failure = PermissionError("denied")
        with self.assertRaisesRegex(NativePinPortError, "retention admission failed"):
            self.port.open(self.vector, self.source, self.context)
        self.assertFalse(self.executor.open)

        self.retention.failure = None
        self.retention.remaining = float("inf")
        with self.assertRaisesRegex(NativePinPortError, "Finite"):
            self.port.open(self.vector, self.source, self.context)
        self.assertFalse(self.executor.open)

    def test_renewal_refusal_poison_closes_handle(self):
        handle = self.port.open(self.vector, self.source, self.context)
        self.retention.remaining = -1
        with self.assertRaisesRegex(NativePinPortError, "Finite"):
            handle.renew()
        self.assertFalse(self.executor.open)
        with self.assertRaisesRegex(NativePinPortError, "closed"):
            handle.close()

    def test_direct_coverage_refusal_closes_and_runs_final_pin_check(self):
        handle = self.port.open(self.vector, self.source, self.context)
        self.retention.failure = PermissionError("admission unavailable")
        with self.assertRaisesRegex(NativePinPortError, "retention admission failed"):
            handle.remaining_lifetime_seconds()
        self.assertFalse(self.executor.open)
        self.assertEqual(sum("assert_active(" in sql for sql, _ in self.executor.calls), 8)
        with self.assertRaisesRegex(NativePinPortError, "closed"):
            handle.remaining_lifetime_seconds()

    def test_pin_vector_drift_refuses_and_closes(self):
        handle = self.port.open(self.vector, self.source, self.context)
        self.executor.rows.pop()
        with self.assertRaises(Exception):
            handle.renew()
        self.assertFalse(self.executor.open)

    def test_close_attempts_pin_release_after_retention_refusal(self):
        handle = self.port.open(self.vector, self.source, self.context)
        self.retention.failure = RuntimeError("admission unavailable")
        with self.assertRaisesRegex(NativePinPortError, "retention admission failed"):
            handle.close()
        self.assertFalse(self.executor.open)


if __name__ == "__main__":
    unittest.main()
