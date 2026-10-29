# SPDX-License-Identifier: MIT OR Apache-2.0
"""Versioned Truss 0.0.51 adapter for the selected R04 feed API.

This verifies the renamed public distribution and exact wheel/runtime profile,
then delegates read-only page validation to the shared 0.49 API adapter.
"""
from .r04_schedule import R04CustodyError


def read_next_transaction(assembly, transaction, source_identity, *,
                          ok_type=None, page_type=None):
    """Read one complete FeedPage through the .51 public API."""
    from .truss_current_catalog import (CURRENT_TRUSS_051_VERSION,
                                        CURRENT_051_EVIDENCE_PROFILE,
                                        installed_current_truss_profile,
                                        installed_truss_distribution)
    try:
        distribution = installed_truss_distribution()
    except ValueError:
        raise R04CustodyError('Installed Truss 0.0.51 public wheel required') from None
    if (distribution.metadata.get('Name') != 'umf-truss'
            or distribution.version != CURRENT_TRUSS_051_VERSION
            or installed_current_truss_profile() != CURRENT_051_EVIDENCE_PROFILE):
        raise R04CustodyError('Exact UMF Truss 0.0.51 wheel/runtime profile required')
    from .r04_truss049_feed import read_next_transaction as read_pinned_api_page
    return read_pinned_api_page(assembly, transaction, source_identity,
                                ok_type=ok_type, page_type=page_type,
                                release_version=CURRENT_TRUSS_051_VERSION)
