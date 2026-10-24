---
ddx:
  id: ashlar.commerce-original-source-local-preflight-20261024
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: ashlar.toolkit-delivery-plan
      kind: informed_by
---
# Original-commerce source local preflight (2026-10-24)

**Result:** the retained original-commerce PostgreSQL endpoint could not be
re-admitted from this local process context. No SQL connection was attempted.
The recorded local native root is absent, no listener was visible on the recorded
ports, and Docker inspection was denied or could not connect to its API. The
container's current running state is unknown. No database, source, ACK, fixture,
or native filesystem mutation was performed.

## Historical selection and provenance

The retained selection in
[`commerce-fabric-delta-native-20261016/selection.json.gz`](../commerce-fabric-delta-native-20261016/selection.json.gz)
identifies `127.0.0.1:25439/truss_e2e`, authentication `isolated-loopback-trust`,
feed `private-original-commerce-fixture`, epoch and batch
`original-commerce-v1`, and source schema `ashlar_ack_source_current15_1`. It
selects separate reader, writer and ACK roles. The selection also pins consumer
`private-commerce-current-1`, source signature
`7ff39b15a87ee5294ee4c659630a6dbe95fddc4c7fbe0bb28d57dacf46afdbef`, scope ID
`b535046c-7977-4d8b-b1ee-b228a0b69868`, and installation ID
`b18fd2fc-12af-43c3-affa-96e7f56dc32d`.

The retained fixture inventory identifies container
`ashlar-private-outbox-current-commerce-20261015-a`, ID
`79d2e5975ec785446b2f2036d92ab41b837a6363e4f6737014fe0f559456963b`, using a
PostgreSQL 17.9 ARM64 image with a one-CPU/512-MiB limit. This is the Ashlar
private outbox fixture; its recorded qualification explicitly excludes Truss and
production source authority. Separate Truss 0.0.22 receipts do not establish
that this fixture is a Truss-owned source database. The historical October 16
publication/materialization evidence remains valid for that run only.

## Read-only checks and outcomes

- `ls -ld` on `/private/tmp/ashlar-current-commerce-preparation-20261015-9pn8k_h7`,
  its `native` directory, and `native/operations.sqlite` reported each path
  absent.
- `lsof -nP -iTCP:15432 -iTCP:25439 -sTCP:LISTEN` exited 1 with no output; no
  process was visible listening on either historical port.
- `/usr/local/bin/docker --host unix:///Users/erik/.orbstack/run/docker.sock
  inspect --format '{{.Id}} {{.State.Status}} {{.State.Running}} {{.Config.Image}}
  {{json .NetworkSettings.Ports}}'
  79d2e5975ec785446b2f2036d92ab41b837a6363e4f6737014fe0f559456963b` exited 1
  with `permission denied while trying to connect to the docker API`.
- `/usr/local/bin/docker --host unix:///Users/erik/.orbstack/run/docker.sock ps
  -a --filter
  id=79d2e5975ec785446b2f2036d92ab41b837a6363e4f6737014fe0f559456963b
  --format '{{.ID}} {{.Names}} {{.Status}} {{.Ports}}'` was denied with the
  same Docker API permission error.
- `ps -axo pid,ppid,stat,command` was denied with `operation not permitted`.
- The root agent separately attempted one escalated, read-only Docker `inspect`
  using the historical container name; it was interrupted after about 20 seconds
  waiting on the OrbStack socket and returned exit 1 with a connection error and
  no state.
- No `pg_isready`, connection attempt, SQL, or credential lookup was run. With
  no visible listener and no verified container identity, a passive SQL read
  could not be safely addressed from this context.

These observations establish local unavailability through the recorded endpoint
at check time; they do not establish whether the container still exists or is
running inside OrbStack. No current source/ACK custody was established.

## R2 boundary

The existing `OriginalCommerceSourceOwner.append_original` accepts only the
driver-owned 21-record R1 batch at source position 1 or its exact readback. The
separate presence preparation proposes a product with
`products.evolution_note` present as a String and a supplier edge to S1, but its
registry IDs are development IDs and it contains no frozen successor request,
accepted Truss IDs, source manifest, ACK, or complete R2 publication vector.
It cannot authorize source mutation or a Fabric carrier.

Before R2 source effects, an authorized source holder must select and admit a
real Truss source/catalog version, confirm how that version accepts the additive
field, freeze exact canonical mutation-request bytes and an independent complete
result oracle, and use Truss's accepted source/mutation path to retain allocated
IDs, source progress, publication manifest/vector and exact ACK readback under
live source custody. The current Ashlar R1-only append owner cannot stand in for
that successor owner. This preflight does not qualify Truss migration or any
current Truss production authority.

## R1 pin reconciliation (offline)

The offline comparison found one incorrect value in the fresh Fabric plan: its
source-signature hash `1b6ce3c1ac4096ae2f5d9faafd74e801e09a47115a7133f6640d08e9fecc7544`
did not match the retained source signature. The correct hash is
`7ff39b15a87ee5294ee4c659630a6dbe95fddc4c7fbe0bb28d57dacf46afdbef`, present
in [the publication report](../publication-current-local-20261016/publication-report.json.gz),
the Fabric [selection](../commerce-fabric-delta-native-20261016/selection.json.gz),
and its [materialization report](../commerce-fabric-delta-native-20261016/report.json.gz)'s
`source_ack_interval`.

The plan's original request hash `8fde1b80674747810602d44893e176eabbfaef23673c58bd215b91dfcd8f6c93`
and manifest hash `e5684ca2d4f9f6471c09eca2b1b2d8be6422a294f4c00755865f0589c8a87ab3`
are correct: they hash the Fabric report's retained `original_request_hex` and
`original_manifest_hex` bytes and match its
[`ack-comparison.json.gz`](../commerce-fabric-delta-native-20261016/ack-comparison.json.gz).
Separately, the original publication report records source-transaction SHA-256
`e7b488a318d562916f55f875f430087077ab263131e9c71754721d637c3c9482`, equal to
the manifest source-progress payload digest; it is distinct from the ACK request
hash. The publication ID, release digest, all four UUID/version pairs, and
position `1` after `0` also match the retained publication and Fabric reports.
This is an offline correction to historical pins only; it does not establish
current source access or authority.
