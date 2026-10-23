# SPDX-License-Identifier: MIT OR Apache-2.0
"""Read-only adapter for the selected Truss 0.0.49 feed API.

This component consumes an already admitted consumer identity and an already
opened read-only transaction. It does not register consumers, mutate source
state, or acknowledge feed progress.
"""
from .r04_schedule import R04CustodyError


MAXIMUM_FACTS = 4096
MAXIMUM_BYTES = 8 * 1024 * 1024


def read_next_transaction(assembly, transaction, source_identity, *,
                          ok_type=None, page_type=None):
    """Return the next whole public FeedPage for a frozen source binding.

    Truss 0.0.49 exposes ``assembly.read_feed_in_transaction(transaction,
    consumer_id=..., maximum_facts=..., maximum_bytes=...)``. The API has no
    cursor argument, so the provider chooses the next transaction at its
    registered checkpoint. ``source_identity`` must come from the frozen,
    independently admitted R04 schedule.
    """
    if ok_type is None or page_type is None:
        try:
            from truss import Ok
            from truss.feed import FeedPage
        except ImportError as error:
            raise R04CustodyError('Selected Truss 0.0.49 public feed types are unavailable') from error
        ok_type = Ok if ok_type is None else ok_type
        page_type = FeedPage if page_type is None else page_type
    required = ('consumer_id', 'registration_id')
    if (type(source_identity) is not dict
            or any(type(source_identity.get(key)) is not str or not source_identity[key]
                   for key in required)
            or not callable(getattr(assembly, 'read_feed_in_transaction', None))):
        raise R04CustodyError('Admitted R04 consumer binding and Truss feed assembly required')
    result = assembly.read_feed_in_transaction(
        transaction,
        consumer_id=source_identity['consumer_id'],
        maximum_facts=MAXIMUM_FACTS,
        maximum_bytes=MAXIMUM_BYTES,
    )
    if not isinstance(result, ok_type) or not isinstance(result.value, page_type):
        raise R04CustodyError('Truss 0.0.49 did not return a public FeedPage')
    page = result.value
    if (page.outcome != 'transaction'
            or page.consumer_id != source_identity['consumer_id']
            or page.registration_id != source_identity['registration_id']):
        raise R04CustodyError('Truss FeedPage differs from the admitted consumer registration')
    return page
