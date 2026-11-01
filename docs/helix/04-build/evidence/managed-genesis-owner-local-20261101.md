---
ddx:
  id: ashlar.evidence.managed-genesis-owner-local-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: TD-009
      kind: informed_by
---

# Managed current genesis owner — focused local result

Sol reviewed Luna's isolated candidate and corrected two integration errors before
landing it: exact reopen after `open_existing()` must not create another
manifest, and `schema_revisions_json` maps Truss installation IDs to catalog
revision strings rather than model hashes. The resulting implementation adds a
typed `ashlar-truss-source-genesis-progress/0.1` marker with exact genesis cursor
bytes and a page-absent sentinel. It separately binds the frozen model-byte
digests, accepted catalog report/view digests, A/B installation revisions,
six-role initial inventory, empty four-role data witnesses and one deterministic
manifest identity. A reopened target may only read back its exact genesis row.

The follow-up adds a pure genesis predecessor verification and first-A-page
transition. It rechecks the immutable row and both selected source markers
before replacing A's page-absent marker with a checkpoint derived from the
caller-supplied, source-owner-admitted native batch; B's marker remains exact.
The batch and checkpoint checks prove internal byte/cursor correspondence, not
who admitted the batch or whether it remains fresh. The selected source owner
must provide those capabilities before the driver can execute a live interval.

The ten focused tests in `test_managed_current_genesis.py` passed in the
selected local Python runtime. They cover exact creation and reopen, absence
of a page checkpoint, refusal of a missing or different manifest, nonempty
data, mismatched model bytes and schema revisions, and forged or altered
transition inputs. No Truss, Spark, Delta or cloud operation ran for these
tests. The caller still owns public A/B registration and acceptance authority,
the native immutable manifest writer and the `/0.2` source-prefix fold. This
result does not qualify managed A1 or ACK.

A subsequent focused driver update accepts the exact durable genesis manifest
as the first A page's predecessor only when the caller supplies complete typed
A/B anchors and the same source-owner-admitted native batch used by graph
admission. It rechecks the row, A checkpoint and unchanged B marker before
preparation; supplied genesis anchors are refused when the durable manifest is
absent or no longer genesis. The paired genesis/publication suite passed 20
tests with one pre-existing private-evidence skip. No managed Spark, Delta,
Truss, cloud or ACK action ran in that suite. Source-owner provenance,
freshness, `/0.2` prefix delivery and live ACK/pin holders remain open.

A separate read-only Truss 0.0.52 source-owner helper now reads the original
selected A/B acceptance reports, complete catalog views, accepted documents,
consumer cursors and first A page through public Truss APIs. It requires the
frozen R04 identities and model pins and passes its exact evidence to the
genesis manifest owner. The public report encodes revision as canonical text;
the typed registration/report API exposes the revision as an integer. The
local-development selector now checks their exact representation correspondence.
The combined source-owner, same-session custody and selector focused suite
passed 20 tests. This was a fake-port and retained-shape check, not a live A/B
source interval. Source freshness during managed effects, native pin renewal,
graph-data prefix composition and live ACK holding remain open.
