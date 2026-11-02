# SPDX-License-Identifier: MIT OR Apache-2.0
"""No-effect fail-closed tests for unavailable A1 Truss lifetime proof."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ashlar_host.managed_a1_truss052_ports import (
    ManagedA1Truss052PortError, ManagedA1Truss052PortOwner,
)
from ashlar_host.truss_source_custody_selection import TrussSourceCustodySelection


def selected_source():
    return TrussSourceCustodySelection(
        page=SimpleNamespace(), maximum_facts=1, maximum_bytes=1,
        request_bytes=b"request", destination_registration_bytes=b"registration",
        ack_scope_bytes=b"scope", destination_id="dest", request_sha256="a" * 64,
        publication_id="publication", manifest_bytes=b"manifest", vector=object(),
        receipt_bytes=b"receipt", schedule_sha256="b" * 64, source="A", identity={})


class ManagedA1Truss052PortTests(unittest.TestCase):
    def setUp(self):
        self.context = object()
        self.owner = ManagedA1Truss052PortOwner(selected_source(), context=self.context)

    def test_wrong_context_refuses_before_public_session_creation(self):
        with patch("ashlar_host.truss052_source_custody.Truss052SourceCustodySession",
                   side_effect=AssertionError("must not construct Truss session")):
            with self.assertRaisesRegex(ManagedA1Truss052PortError,
                                        "Original Truss owner context"):
                self.owner.source_custody_port.open(selection=object(),
                    request_bytes=b"request", manifest_bytes=b"manifest",
                    receipt_bytes=b"receipt", vector=object(), context=object())

    def test_matching_context_still_refuses_without_live_custody_proof(self):
        with patch("ashlar_host.truss052_source_custody.Truss052SourceCustodySession",
                   side_effect=AssertionError("must not construct Truss session")):
            with self.assertRaisesRegex(ManagedA1Truss052PortError,
                                        "verified live custody lifetime"):
                self.owner.source_custody_port.open(selection=object(),
                    request_bytes=b"request", manifest_bytes=b"manifest",
                    receipt_bytes=b"receipt", vector=object(), context=self.context)

    def test_ack_ports_refuse_without_source_handle_proof(self):
        with self.assertRaisesRegex(ManagedA1Truss052PortError,
                                    "verified live custody lifetime"):
            self.owner.protected_ack_port.commit(b"request", b"manifest", b"receipt",
                self.context, deadline_monotonic=100.0)
        with self.assertRaisesRegex(ManagedA1Truss052PortError,
                                    "verified live custody lifetime"):
            self.owner.protected_ack_port.reconcile(b"request", b"manifest", b"receipt",
                self.context, deadline_monotonic=100.0)


if __name__ == "__main__":
    unittest.main()
