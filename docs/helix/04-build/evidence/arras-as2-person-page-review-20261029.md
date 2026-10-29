# AS-2 person-page owner review — 2026-10-29

This Sol review follows local candidate `c83ae1a2`. Luna inspected the
person-page owner against the original Arras Backend02 preparation and found
that `authorize_page` could run before current custody renewal. The owner now
renews runtime, native inventory and policy before invoking the request policy.

The review also rejected a proposed publisher-style `table_owner == current
person` check. An authorized person may have UC SELECT and effective row/column
rights without owning the six publisher tables. Native SQL still runs under the
current person's Spark session; the mandatory policy and exact inventory,
manifest and publication vector are renewed inside the held read.

Focused check: `PYTHONPATH=src:tests .venv/bin/python -m unittest
tests.test_managed_current_person_read tests.test_managed_current_query` passed
21 tests in 0.055 seconds. The new control checks that custody renewal precedes
request authorization. This is local evidence only; a real current-person UC
rights policy and native managed before/after case remain release gates. The
[exact review manifest](arras-as2-person-page-review-20261029-manifest.json)
binds these changed files. No cloud job, source ACK or remote push was run.
