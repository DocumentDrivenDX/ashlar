from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import truss
from truss import CatalogViewUnavailable

from catalog_view_diagnostic import require_catalog_view_bytes


class CatalogViewDiagnosticTests(unittest.TestCase):
    def test_available_view_returns_original_bytes(self):
        class Available:
            def __init__(self, view):
                self.view = view

        with patch.object(truss, "CatalogViewAvailable", Available):
            self.assertEqual(
                require_catalog_view_bytes(
                    Available(SimpleNamespace(original_bytes=b'{"ok":true}'))
                ),
                b'{"ok":true}',
            )

    def test_unavailable_view_preserves_reason_before_parsing(self):
        with self.assertRaisesRegex(
            RuntimeError, r"Complete public \.48 catalog view unavailable \(reason=resource\)"
        ):
            require_catalog_view_bytes(CatalogViewUnavailable(reason="resource"))

    def test_unexpected_result_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "unexpected result"):
            require_catalog_view_bytes(object())


if __name__ == "__main__":
    unittest.main()
