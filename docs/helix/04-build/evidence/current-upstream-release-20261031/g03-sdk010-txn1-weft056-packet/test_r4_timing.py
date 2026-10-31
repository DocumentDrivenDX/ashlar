import json
import unittest
from unittest.mock import patch

from r4_timing import MAX_BYTES, MAX_EVENTS, PROFILE, StageTimings, install_sdk010_transaction_hooks


class TimingTests(unittest.TestCase):
    def test_records_order_elapsed_and_failure_without_payload(self):
        clock = iter([10.0, 10.0, 10.25, 11.0, 11.5]).__next__
        timings = StageTimings(clock)
        with timings.measure("metadata.reservation"):
            pass
        with self.assertRaisesRegex(RuntimeError, "stop"):
            with timings.measure("metadata.operation_mutation"):
                raise RuntimeError("stop")
        value = json.loads(timings.to_bytes())
        self.assertEqual(value["profile"], PROFILE)
        self.assertEqual([event["stage"] for event in value["events"]],
                         ["metadata.reservation", "metadata.operation_mutation"])
        self.assertEqual([event["durationMs"] for event in value["events"]], [250, 500])
        self.assertEqual([event["outcome"] for event in value["events"]], ["ok", "error"])
        self.assertNotIn("stop", timings.to_bytes().decode())

    def test_rejects_unregistered_stage_and_event_overflow(self):
        timings = StageTimings(lambda: 1.0)
        with self.assertRaisesRegex(ValueError, "unknown timing stage"):
            with timings.measure("sql.text"):
                pass
        timings.events = [{"stage": "metadata.reservation", "startOffsetMs": 0,
                           "durationMs": 0, "outcome": "ok"}] * MAX_EVENTS
        with self.assertRaisesRegex(ValueError, "event limit"):
            with timings.measure("metadata.reservation"):
                pass
        self.assertLessEqual(len(timings.to_bytes()), MAX_BYTES)

    def test_runtime_hooks_measure_and_restore_only_selected_boundaries(self):
        class Owner:
            def reserve_delta4_prepared_phase(self):
                return "reserved"
        class Source:
            def __init__(self, **_kwargs):
                self.ready = True
        class Transport:
            def metadata_mutation(self):
                return "committed"
            def metadata_recover(self):
                return "recovered"
            def observe_delta4_metadata_commit(self):
                return "observed"
        class Coordinator:
            def reserve_delta4_prepared_phase(self):
                return "reserved"
            def prepare_delta4_native_prepared_phase(self):
                return "phases"

        timings = StageTimings()
        owners = patch.multiple(
            "ashlar_host.configured_source_local",
            ConfiguredLocalSource=Source,
        )
        with owners, patch.multiple(
            "ashlar_host.configured_local_delta4_publication",
            ConfiguredLocalDelta4WriterAuthority=Coordinator,
        ), patch.multiple(
            "ashlar_host.delta_custody", LocalDeltaTransport=Transport,
        ):
            # install imports module classes; replace these symbols before use.
            restore = install_sdk010_transaction_hooks(timings)
            try:
                self.assertEqual(Source(sdk010_admission=object()).ready, True)
                self.assertEqual(Coordinator().prepare_delta4_native_prepared_phase(), "phases")
                self.assertEqual(Transport().metadata_mutation(), "committed")
                self.assertEqual(len(timings.events), 3)
            finally:
                restore()
            self.assertEqual(Coordinator.prepare_delta4_native_prepared_phase.__name__,
                             "prepare_delta4_native_prepared_phase")


if __name__ == "__main__":
    unittest.main()
