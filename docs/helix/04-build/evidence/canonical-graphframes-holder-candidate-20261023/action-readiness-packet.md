# Canonical `.4` GraphFrames action readiness packet — 2026-10-23

This packet defines the next small synthetic action slice for TD-008. It is an
offline design/evidence bridge from the metadata-only holder to one bounded
GraphFrames one-hop action. It authorizes no Spark execution, scale trial,
publication, Unity Catalog access, source progress, protected ACK, or production
authority. The 1B/5B run remains held by the R05 review gate.

## Evidence carried forward

- TD-008 requires a frozen synthetic generator and independent oracle, exact
  pinned current-table identities, a bounded edge-distinct action, and no full
  frame collection.
- The current canonical holder candidate binds descriptor fields and generated
  carrier metadata, observes the two Delta UUID/version/schema/protocol tuples,
  and provides lazy `VERSION AS OF` frames. Its descriptor tests use placeholder
  digests and illustrative formula/oracle values; those declarations have not
  been audited against or executed by the canonical generator.
- The measured 45M-node/225M-edge local fixture is synthetic, has redundant
  `source_position=id`, and is not production source evidence. Its measured
  write projection is 18.05 minutes at the stated 80% curve, so the 1B/5B write
  remains held. See [R05 reassessment](../r05-storage-throughput-reassessment-20261023/README.md)
  and the [18-core receipt](../current-upstream-release-20261021/local-current/canonical-carrier-xxhash64-18core-calibration-20261021.json).
- The existing actual GraphFrames receipt proves only the tiny three-node,
  two-parallel-edge fixture under its recorded Spark/Delta/GraphFrames runtime.
  It does not admit the canonical carrier holder or prove retained-file custody.

## Minimum frozen descriptor before execution

Create a fixture-specific descriptor from the exact retained generator/source
artifacts, not from caller-supplied placeholder strings. Freeze and hash:

1. Generator repository revision and exact generator file bytes; source artifact
   bytes or an immutable source receipt; selected UMF revision and generated
   carrier-definition bytes.
2. Canonical profile and runtime selection; exact row counts; full deterministic
   node and edge value formulas (including signed-64 bounds, source fields,
   relation identity, endpoints, parallel-edge multiplicity, and null/payload
   rules); selected seed; and an independently authored expected edge-occurrence
   bag for that seed.
3. Expected output schema and explicit action cap. The seed must have an
   independently proven degree no greater than the cap. The action collects at
   most `cap + 1`; seeing `cap + 1` refuses as incomplete. Equality with the
   independently frozen occurrence bag, including distinct edge identity, is
   required for pass.

The oracle must be derived separately from the generator implementation. A
second call to the generator, a Spark query over the same tables, row counts,
Delta statistics, or GraphFrames output itself cannot serve as the oracle.
Descriptor creation refuses if any referenced bytes, revision, counts, schema,
formula, seed, cap, or oracle digest changes. No whole-table uniqueness join is
needed: uniqueness and endpoint validity are mathematical properties of the
frozen formulas, while persisted counts and the bounded action are runtime
witnesses. Any claim that the materialized large fixture followed those
formulas needs a bounded distributed witness plan recorded separately; the
one-hop action alone does not prove every row.

## Custody window and exact runtime admission

Replace the metadata lease as an action authority with one context-managed
synthetic-use lease. It must:

- Acquire a nonblocking OS advisory lock on a stable lock inode under the
  canonical private fixture root before opening either table, and hold it
  through final renewal. Resolve and verify the fixture root and lock path
  canonically; do not unlink or replace the lock inode during normal use. Every
  owned generator, table mutator, vacuum/retention operation, and cleanup path
  must acquire that same lock before changing or removing fixture files. Lock
  contention or a changed root/lock identity refuses. This is cooperative
  exclusion among owned processes, not a universal filesystem fence against
  actors that ignore the lock; therefore keep the root private to this run and
  state that custody boundary in the receipt. Delta metadata re-observation
  alone is not exclusivity.
- Bind the canonical descriptor digest, both table paths, exact UUID/version,
  schema digest, protocol, and a deterministic inventory of every active data
  file in the selected snapshots. For each file record canonical path, size,
  modification time at filesystem precision, and filesystem identity (device
  and inode/file key where available); include a content digest or stronger
  immutable object identity when the filesystem exposes one. Check that every
  inventoried file exists and matches before yielding frames, and recompute the
  same inventory after the action while still holding the lock. Cap inventory
  entries and serialized metadata bytes; refuse when either bound is exceeded.
  Changed, missing, added, removed, or replaced files, changed table heads, or
  lease expiry refuses the receipt. Retain the files and required Delta log
  records throughout the interval. This is bounded metadata/file-stat work, not
  a collect of carrier rows; it detects ordinary replacement, while the
  cooperative lock and private-root boundary address owned concurrent writes.
- Admit the actual running runtime before constructing `GraphFrame`: exact
  Spark, Delta, GraphFrames and JDK versions plus resolved GraphFrames/Delta JAR
  paths and SHA-256 values must match the reviewed local profile. Read versions
  from the live runtime/class locations, verify the JAR bytes, and instantiate
  the GraphFrames class from the admitted JAR. A Python distribution version or
  configured version string alone is insufficient. Recheck runtime/JAR
  identities before releasing the action result.
- Keep authority labels explicit: `sourceProgress=absent`,
  `protectedAck=absent`, `publication=absent`, `unityCatalog=absent`,
  `productionAuthority=absent`.

The first action uses the two exact version-pinned frames and transient `.4`
GraphFrames views from TD-008. It selects one frozen seed, applies the exact
one-hop predicate, selects only the edge-distinct result columns, and collects
no more than `cap + 1` rows. It does not cache the full graph, persist a second
graph, convert to GraphX, run an unbounded motif, or collect either input frame.
The receipt records the descriptor digest, lock identity/window, table pins and
snapshot inventory digests, actual runtime/JAR identities, selected seed, cap,
bounded result bag, checksums of evidence inputs, renewal outcomes, and refusal
or pass status. No truncated answer may be labeled exact.

## Exact next Luna implementation slice

In `src/ashlar_host/pinned_delta_graphframes.py`, add a separate canonical
one-hop entrypoint and its private action gate. Keep the tiny smoke entrypoint
unchanged. The new path should consume an audited canonical descriptor, acquire
the exclusive retained-file lease, perform actual runtime/JAR admission, map
the version-pinned frames to transient views, execute the capped seed action,
compare the complete edge-occurrence bag to the frozen oracle, renew all gates,
and emit a bounded evidence object. It must not alter `src/` outside this
module, `pyproject.toml`, table schemas, the generator, publisher behavior, or
public commands/receipts. Do not wire this path into a general publication API.

The next offline implementation review should include focused refusal coverage
for descriptor drift, lock contention, missing/replaced retained files, changed
Delta head/schema/protocol, runtime and JAR mismatch, expired lease, cap
overflow, oracle mismatch, and a successful bounded action with no input-frame
collect. Actual Spark execution remains a separately authorized step after
review; the implementation slice itself should not start Spark, PostgreSQL, or
cloud services.

## Stop conditions and qualification

Any missing proof or runtime mismatch refuses before action. Any post-action
custody renewal failure suppresses the success result. Keep partial diagnostics
without converting them to a pass. This slice qualifies only the selected
canonical synthetic fixture and exact admitted local runtime/JAR tuple after an
actual action receipt exists. It does not change the R05 hold, qualify the
45M/225M sample as source data, or establish full-scale algorithmic performance.
