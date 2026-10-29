# SPDX-License-Identifier: MIT OR Apache-2.0
import unittest

from ashlar_host._source_holder_interval_proposal import (
    SourceHolderAckUncertain,
    SourceHolderAckIncomplete,
    SourceHolderCloseError,
    commit_and_reconcile_protected_ack_proposal,
    hold_source_holder_interval_proposal,
)


class _Handle:
    def __init__(self, role, events, *, renew_error=None, close_error=None,
                 renew_result=None, close_result=None):
        self.role = role
        self.events = events
        self.renew_error = renew_error
        self.close_error = close_error
        self.renew_result = renew_result
        self.close_result = close_result
        self.remaining = 60

    def renew(self):
        self.events.append(self.role + ".renew")
        if self.renew_error is not None:
            raise self.renew_error
        return self.renew_result

    def close(self):
        self.events.append(self.role + ".close")
        if self.close_error is not None:
            raise self.close_error
        return self.close_result

    def remaining_lifetime_seconds(self):
        return self.remaining


class _Port:
    def __init__(self, role, events, handle, *, open_error=None):
        self.role = role
        self.events = events
        self.handle = handle
        self.open_error = open_error

    def open(self, *arguments):
        self.events.append(self.role + ".open")
        if self.open_error is not None:
            raise self.open_error
        self.arguments = arguments
        return self.handle


class SourceHolderIntervalProposalTests(unittest.TestCase):
    def setup_ports(self, *, failures=None):
        failures = failures or {}
        events = []
        handles = {
            role: _Handle(role, events,
                renew_error=failures.get(role + ".renew"),
                close_error=failures.get(role + ".close"),
                renew_result=failures.get(role + ".renew_result"),
                close_result=failures.get(role + ".close_result"))
            for role in ("managed", "pins", "source")
        }
        ports = (
            _Port("managed", events, handles["managed"], open_error=failures.get("managed.open")),
            _Port("pins", events, handles["pins"], open_error=failures.get("pins.open")),
            _Port("source", events, handles["source"], open_error=failures.get("source.open")),
        )
        return events, handles, ports

    def hold(self, ports, *, context=None):
        context = object() if context is None else context
        return hold_source_holder_interval_proposal(
            *ports, receipt_bytes=b"receipt", vector=object(), source_selection=object(),
            request_bytes=b"request", context=context,
        )

    def test_opens_in_order_renews_all_before_body_and_closes_reverse(self):
        events, _, ports = self.setup_ports()
        with self.hold(ports) as interval:
            self.assertEqual(events, [
                "managed.open", "pins.open", "source.open",
                "managed.renew", "pins.renew", "source.renew",
            ])
            self.assertFalse(hasattr(interval, "commit"))
            events.append("body")
        self.assertEqual(events, [
            "managed.open", "pins.open", "source.open",
            "managed.renew", "pins.renew", "source.renew", "body",
            "source.close", "pins.close", "managed.close",
        ])

    def test_open_failure_closes_only_acquired_handles_in_reverse(self):
        events, _, ports = self.setup_ports(failures={"source.open": RuntimeError("open failed")})
        with self.assertRaisesRegex(RuntimeError, "open failed"):
            with self.hold(ports):
                self.fail("body must not run")
        self.assertEqual(events, [
            "managed.open", "pins.open", "source.open", "pins.close", "managed.close",
        ])

    def test_renew_failure_preserves_primary_and_closes_every_handle(self):
        renew_error = RuntimeError("renew failed")
        events, _, ports = self.setup_ports(failures={
            "pins.renew": renew_error,
            "source.close": OSError("close failed"),
        })
        with self.assertRaises(RuntimeError) as caught:
            with self.hold(ports):
                self.fail("body must not run")
        self.assertIs(caught.exception, renew_error)
        self.assertEqual(
            [(f.handle, f.error_type) for f in caught.exception.source_holder_close_failures],
            [("truss_source", "OSError")],
        )
        self.assertEqual(events[-3:], ["source.close", "pins.close", "managed.close"])

    def test_body_failure_is_preserved_and_every_close_is_attempted(self):
        body_error = LookupError("body failed")
        events, _, ports = self.setup_ports(failures={
            "source.close": OSError("source close"),
            "managed.close": RuntimeError("managed close"),
        })
        with self.assertRaises(LookupError) as caught:
            with self.hold(ports):
                events.append("body")
                raise body_error
        self.assertIs(caught.exception, body_error)
        self.assertEqual(
            [(f.handle, f.error_type) for f in body_error.source_holder_close_failures],
            [("truss_source", "OSError"), ("managed_publication", "RuntimeError")],
        )
        self.assertEqual(events[-3:], ["source.close", "pins.close", "managed.close"])

    def test_close_only_failure_reports_all_reverse_order_failures(self):
        events, _, ports = self.setup_ports(failures={
            "source.close": OSError("source close"),
            "managed.close": RuntimeError("managed close"),
        })
        with self.assertRaises(SourceHolderCloseError) as caught:
            with self.hold(ports):
                events.append("body")
        self.assertEqual(
            [(f.handle, f.error_type) for f in caught.exception.failures],
            [("truss_source", "OSError"), ("managed_publication", "RuntimeError")],
        )
        self.assertEqual(events[-3:], ["source.close", "pins.close", "managed.close"])

    def test_explicit_renew_all_inside_body_rechecks_all_handles(self):
        events, _, ports = self.setup_ports()
        with self.hold(ports) as interval:
            events.append("before-second-renewal")
            interval.renew_all()
        self.assertEqual(events[6:10], [
            "before-second-renewal", "managed.renew", "pins.renew", "source.renew",
        ])

    def test_non_none_port_results_are_ignored(self):
        events, _, ports = self.setup_ports(failures={
            "managed.renew_result": False,
            "source.close_result": False,
        })
        with self.hold(ports):
            events.append("body")
        self.assertEqual(events[-4:], [
            "body", "source.close", "pins.close", "managed.close",
        ])

    def test_reused_handle_is_refused_and_closed_once(self):
        events, _, ports = self.setup_ports()
        ports[1].handle = ports[0].handle
        with self.assertRaisesRegex(ValueError, "Three distinct"):
            with self.hold(ports):
                self.fail("body must not run")
        self.assertEqual(events, ["managed.open", "pins.open", "managed.close"])

    def test_retained_interval_cannot_be_renewed_after_context_exit(self):
        events, _, ports = self.setup_ports()
        with self.hold(ports) as interval:
            events.append("body")
        before = list(events)
        with self.assertRaisesRegex(ValueError, "interval is closed"):
            interval.renew_all()
        self.assertEqual(events, before)

    def test_protected_ack_renews_immediately_commits_and_reconciles_exact_bytes(self):
        events, _, ports = self.setup_ports()
        class Ack:
            def commit(self, *args):
                events.append(("ack.commit", args))
            def reconcile(self, *args):
                events.append(("ack.reconcile", args))
                return (args[0], args[1], args[2])
        context = object()
        with self.hold(ports, context=context) as interval:
            commit_and_reconcile_protected_ack_proposal(
                interval, Ack(), request_bytes=b"request", manifest_bytes=b"manifest",
                receipt_bytes=b"receipt", context=context,
            )
        self.assertEqual(events[6:12], [
            "managed.renew", "pins.renew", "source.renew",
            ("ack.commit", (b"request", b"manifest", b"receipt", context)),
            ("ack.reconcile", (b"request", b"manifest", b"receipt", context)),
            "source.close",
        ])

    def test_successful_ack_is_one_shot_per_interval(self):
        events, _, ports = self.setup_ports()
        context = object()
        class Ack:
            def commit(self, *args): events.append("ack.commit")
            def reconcile(self, *args): return (args[0], args[1], args[2])
        with self.hold(ports, context=context) as interval:
            args = dict(request_bytes=b"request", manifest_bytes=b"manifest",
                        receipt_bytes=b"receipt", context=context)
            commit_and_reconcile_protected_ack_proposal(interval, Ack(), **args)
            with self.assertRaisesRegex(ValueError, "already attempted"):
                commit_and_reconcile_protected_ack_proposal(interval, Ack(), **args)
        self.assertEqual(events.count("ack.commit"), 1)

    def test_uncertain_commit_is_one_shot_per_interval(self):
        events, _, ports = self.setup_ports()
        context = object()
        class Ack:
            def commit(self, *args):
                events.append("ack.commit")
                raise OSError("unknown outcome")
            def reconcile(self, *args): self.fail("reconcile not reached")
        with self.hold(ports, context=context) as interval:
            args = dict(request_bytes=b"request", manifest_bytes=b"manifest",
                        receipt_bytes=b"receipt", context=context)
            for expected in (SourceHolderAckUncertain, ValueError):
                with self.assertRaises(expected):
                    commit_and_reconcile_protected_ack_proposal(interval, Ack(), **args)
        self.assertEqual(events.count("ack.commit"), 1)

    def test_ack_reconciliation_failure_is_uncertain_and_closes_all(self):
        events, _, ports = self.setup_ports()
        context = object()
        class Ack:
            def commit(self, *args): events.append("ack.commit")
            def reconcile(self, *args):
                events.append("ack.reconcile")
                raise OSError("readback failed")
        with self.assertRaises(SourceHolderAckUncertain):
            with self.hold(ports, context=context) as interval:
                commit_and_reconcile_protected_ack_proposal(
                    interval, Ack(), request_bytes=b"request", manifest_bytes=b"manifest",
                    receipt_bytes=b"receipt", context=context,
                )
        self.assertEqual(events[-3:], ["source.close", "pins.close", "managed.close"])

    def test_nonmatching_reconcile_readback_is_uncertain(self):
        events, _, ports = self.setup_ports()
        context = object()
        class Ack:
            def commit(self, *args): events.append("ack.commit")
            def reconcile(self, *args): return (b"other", args[1], args[2])
        with self.assertRaises(SourceHolderAckUncertain):
            with self.hold(ports, context=context) as interval:
                commit_and_reconcile_protected_ack_proposal(
                    interval, Ack(), request_bytes=b"request", manifest_bytes=b"manifest",
                    receipt_bytes=b"receipt", context=context,
                )

    def test_ack_bytes_must_match_opened_request_and_receipt(self):
        events, _, ports = self.setup_ports()
        context = object()
        class Ack:
            def commit(self, *args): self.fail("must not commit")
            def reconcile(self, *args): self.fail("must not reconcile")
        with self.assertRaisesRegex(ValueError, "differ from source-holder opening"):
            with self.hold(ports, context=context) as interval:
                commit_and_reconcile_protected_ack_proposal(
                    interval, Ack(), request_bytes=b"changed", manifest_bytes=b"manifest",
                    receipt_bytes=b"receipt", context=context,
                )

    def test_nonfinite_window_and_live_coverage_are_refused(self):
        for value in (float("nan"), float("inf")):
            events, _, ports = self.setup_ports()
            with self.assertRaises(ValueError):
                with hold_source_holder_interval_proposal(
                    *ports, receipt_bytes=b"receipt", vector=object(),
                    source_selection=object(), request_bytes=b"request", context=object(),
                    ack_window_seconds=value,
                ):
                    self.fail("nonfinite window admitted")
        events, handles, ports = self.setup_ports()
        context = object()
        with self.hold(ports, context=context) as interval:
            handles["managed"].remaining = float("inf")
            with self.assertRaisesRegex(ValueError, "does not cover ACK window"):
                commit_and_reconcile_protected_ack_proposal(
                    interval, object(), request_bytes=b"request", manifest_bytes=b"manifest",
                    receipt_bytes=b"receipt", context=context,
                )

    def test_close_failure_after_exact_reconciliation_is_incomplete(self):
        events, _, ports = self.setup_ports(failures={"pins.close": OSError("close")})
        context = object()
        class Ack:
            def commit(self, *args): events.append("ack.commit")
            def reconcile(self, *args): return (args[0], args[1], args[2])
        with self.assertRaises(SourceHolderAckIncomplete):
            with self.hold(ports, context=context) as interval:
                commit_and_reconcile_protected_ack_proposal(
                    interval, Ack(), request_bytes=b"request", manifest_bytes=b"manifest",
                    receipt_bytes=b"receipt", context=context,
                )

    def test_insufficient_coverage_refuses_before_body_and_closes(self):
        events, _, ports = self.setup_ports()
        ports[1].handle.remaining_lifetime_seconds = lambda: 29
        with self.assertRaisesRegex(ValueError, "does not cover ACK window"):
            with self.hold(ports):
                self.fail("body must not run")
        self.assertEqual(events[-3:], ["source.close", "pins.close", "managed.close"])


if __name__ == "__main__":
    unittest.main()
