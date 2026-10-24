# SPDX-License-Identifier: MIT OR Apache-2.0
"""Versioned Truss 0.0.50 adapter for the selected R04 feed API.

The .50 package receipt qualifies import/signature coexistence only. This
adapter verifies the installed wheel/profile before delegating to the shared
read-only page-shape validator; it grants no registration, ACK or publication
authority.
"""
import importlib.metadata

from .r04_schedule import R04CustodyError


def read_next_transaction(assembly, transaction, source_identity, *,
                          ok_type=None, page_type=None):
    """Read one complete FeedPage through the .50 public API at its checkpoint."""
    from .truss_current_catalog import (CURRENT_TRUSS_050_VERSION,
                                        CURRENT_050_EVIDENCE_PROFILE,
                                        installed_current_truss_profile)
    try:
        version = importlib.metadata.version('truss-toolkit')
    except importlib.metadata.PackageNotFoundError:
        raise R04CustodyError('Installed Truss 0.0.50 public wheel required') from None
    if (version != CURRENT_TRUSS_050_VERSION
            or installed_current_truss_profile() != CURRENT_050_EVIDENCE_PROFILE):
        raise R04CustodyError('Exact Truss 0.0.50 wheel/runtime profile required')
    from .r04_truss049_feed import read_next_transaction as read_pinned_api_page
    return read_pinned_api_page(assembly, transaction, source_identity,
                                ok_type=ok_type, page_type=page_type)
