# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused checks for the .53 A1 custody adapter gate."""
import unittest
from unittest.mock import patch

from ashlar_host.truss052_source_custody import Truss052CustodyError
from ashlar_host.truss053_source_custody import (
    Truss053CustodyError, Truss053SourceCustodySession,
)
from test_truss052_source_custody import _open_selection, _selection


class Truss053SourceCustodyTests(unittest.TestCase):
    def test_exact_fresh_profile_admits_same_session_prepublication_selection(self):
        selection = _open_selection(_selection())
        with patch("ashlar_host.truss_current_catalog.installed_truss_053_fresh_source_custody_profile"):
            session = Truss053SourceCustodySession(
                connection_factory=lambda context: None,
                selection=selection, context=object())
        self.assertIs(session.selection, selection)
        self.assertTrue(session._prepublication)

    def test_profile_refusal_happens_before_connection_factory(self):
        opened = []
        with patch("ashlar_host.truss_current_catalog.installed_truss_053_fresh_source_custody_profile",
                   side_effect=ValueError("wrong package or pins")):
            with self.assertRaisesRegex(Truss053CustodyError, "Exact installed Truss 0.0.53"):
                Truss053SourceCustodySession(
                    connection_factory=lambda context: opened.append(context),
                    selection=_open_selection(_selection()), context=object())
        self.assertEqual(opened, [])


if __name__ == "__main__":
    unittest.main()
