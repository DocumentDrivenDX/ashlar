# R15 empty-view diagnostic candidate

This separate candidate demonstrates the minimal helper correction suggested by
the failed R15 read. It distinguishes `CatalogViewAvailable` from
`CatalogViewUnavailable` and preserves the latter's reason instead of replacing
it with `b''` before JSON parsing. The focused test exercises available,
unavailable, and unexpected result variants against the pinned Truss API types
without PostgreSQL or native execution.

This candidate does not repair or establish the underlying view availability.
The executed `source_048_readonly_helper.py` and its hash-bound R15 packet remain
unchanged. Any later execution must use a newly reviewed exact packet and a new
human approval bound to its updated helper/runtime inputs; the consumed R15
approval cannot authorize it.
