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
must provide those capabilities before this helper can be wired to a driver.

The ten focused tests in `test_managed_current_genesis.py` passed in the
selected local Python runtime. They cover exact creation and reopen, absence
of a page checkpoint, refusal of a missing or different manifest, nonempty
data, mismatched model bytes and schema revisions, and forged or altered
transition inputs. No Truss, Spark, Delta or cloud operation ran for these
tests. The caller still owns public A/B registration and acceptance authority,
the native immutable manifest writer, incremental driver wiring and the
`/0.2` source-prefix fold. This result does not qualify managed A1 or ACK.
