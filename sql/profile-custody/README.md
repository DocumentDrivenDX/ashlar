This isolated PostgreSQL17.9 custody store retains immutable original profile
artifact bytes and a separately bounded source bundle. It supplies no active
registration, implementation execution, acceptance support or Truss ready marker.

Install `01-postgresql.sql` once in the authorized private development database.
Its transaction creates three NOLOGIN roles, one owner-controlled schema/table
and one protected retain function, revoking PUBLIC execution. The writer can
retain originals through the function; the reader can inspect stored originals.
Neither has direct insert/update/delete privileges. The owner/superuser boundary
remains trusted; these checks do not qualify arbitrary global/admin/proxy paths.

The definition and source bundle both participate in exact-repeat equality.
Changed bytes for an existing identity/version refuse; a new version is required.
Repeat returns the original native role, xid8 and timestamp, never a new context.
The role capture is the actual session-selected role for the direct entry path;
nested SECURITY DEFINER caller/profile qualification is outside this scope.
No TTL, deletion, execution/admission flag or automatic expiry exists.

`02-native-check.sql` exercises binary custody, repeat, changed-definition/bundle
refusals, direct writer DML denial and reader retain denial, then rolls its fixture
back. `tools/retain_profile_custody.py` stores a bounded original candidate set and
records native receipts. Its source bundle contains original archive and producer
artifacts only; complete transitive dependency recognition is still unavailable.
The verified reader/replay receipts and actual owner/grant observations live in
SPIKE-001-table-layout/out/native/profile_custody_20261008 and its replay sibling.
The Truss head remains zero. Do not use these records as admission authority.
