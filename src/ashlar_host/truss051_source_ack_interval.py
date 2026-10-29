# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fail-closed managed orchestration boundary for Truss 0.0.51 source ACK.

The read-only reopen observation produced by the managed current Spark reader
is not a live managed-read or native-pin handle. It must never authorize a
source ACK. TrussFeedSessions retains separately callable, read-only .51 source
observations; effectful composition stays unavailable until the actual live
owner interfaces have been designed and admitted.
"""

from .truss_feed_sessions import TrussSessionError


PROFILE = 'ashlar-truss051-source-ack-interval/0.1'


def acknowledge_post_verifier_source_interval(*args, **kwargs):
    """Refuse until live managed-read and native-pin owners are available.

    In particular, no constructible offline receipt-reopen observation can
    authorize an effectful route to Truss receipt ACK.
    """
    raise TrussSessionError(
        'Truss .51 source ACK orchestration unavailable: live managed-read and '
        'native-pin owner handles are required; offline reopen observations '
        'cannot authorize source ACK')
