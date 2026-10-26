"""Diagnostic-only candidate for the R15 retained .48 catalog-view read."""
from __future__ import annotations


def require_catalog_view_bytes(result) -> bytes:
    """Return available view bytes or preserve the API's unavailable reason."""
    from truss import CatalogViewAvailable, CatalogViewUnavailable

    if isinstance(result, CatalogViewAvailable):
        return result.view.original_bytes
    if isinstance(result, CatalogViewUnavailable):
        raise RuntimeError(
            f"Complete public .48 catalog view unavailable (reason={result.reason})"
        )
    raise RuntimeError("Public .48 catalog view returned an unexpected result")
