# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused checks for the .53 A1 custody adapter gate."""
import unittest
from types import MappingProxyType
from unittest.mock import patch

from ashlar_host.truss_current_catalog import (
    CURRENT_055_FRESH_CUSTODY_PROFILE,
    CURRENT_TRUSS_055_FRESH_RUNTIME_MANIFEST_SHA256,
    CURRENT_TRUSS_055_FRESH_WHEEL_SHA256,
)
from ashlar_host.truss052_source_custody import Truss052CustodyError
from ashlar_host.truss053_source_custody import (
    Truss053A1SourceHolder, Truss053CustodyError, Truss053SourceCustodySession,
)
from test_truss052_source_custody import _open_selection, _selection


class Truss053SourceCustodyTests(unittest.TestCase):
    def test_a1_resume_rejects_wrong_settlement_before_phase_change(self):
        holder = object.__new__(Truss053A1SourceHolder)
        holder._closed = False
        holder.phase = "a0"
        holder.genesis = None
        holder.genesis_bytes = None
        holder.session = None
        holder.snapshot_bytes = None
        holder.anchor = None
        holder.bound_selection = None
        holder._a0_settlement = None
        holder._a0_page_next = None
        holder.source_selections = {"A": object(), "B": object()}
        holder._advance_a1_callback = lambda _: self.fail("resume must not seed")
        with self.assertRaisesRegex(Truss053CustodyError,
                                    "Fresh unopened source holder"):
            holder.resume_a1_after_matched_a0(object(), object())
        self.assertEqual(holder.phase, "a0")
        self.assertIsNone(holder._a0_page_next)

    def test_current_055_fresh_profile_admits_same_session_prepublication_selection(self):
        selection = _open_selection(_selection())
        pins = MappingProxyType({"version": "0.0.55",
            "wheel_sha256": CURRENT_TRUSS_055_FRESH_WHEEL_SHA256,
            "manifest_sha256": CURRENT_TRUSS_055_FRESH_RUNTIME_MANIFEST_SHA256,
            "source_custody_profile": CURRENT_055_FRESH_CUSTODY_PROFILE})
        with patch("ashlar_host.truss_current_catalog.installed_fresh_truss_runtime_pins",
                   return_value=pins):
            session = Truss053SourceCustodySession(
                connection_factory=lambda context: None,
                selection=selection, context=object())
        self.assertIs(session.selection, selection)
        self.assertTrue(session._prepublication)

    def test_profile_refusal_happens_before_connection_factory(self):
        opened = []
        with patch("ashlar_host.truss_current_catalog.installed_fresh_truss_runtime_pins",
                   side_effect=ValueError("wrong package or pins")):
            with self.assertRaisesRegex(Truss053CustodyError, "Exact installed current-fresh Truss"):
                Truss053SourceCustodySession(
                    connection_factory=lambda context: opened.append(context),
                    selection=_open_selection(_selection()), context=object())
        self.assertEqual(opened, [])


if __name__ == "__main__":
    unittest.main()
