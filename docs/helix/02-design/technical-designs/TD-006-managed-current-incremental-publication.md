---
ddx:
  id: TD-006
  type: technical-design
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
  - id: ADR-001
    kind: informed_by
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-003
    kind: informed_by
  - id: CONTRACT-005
    kind: informed_by
  - id: TD-001
    kind: informed_by
---

# Managed current-core incremental publication candidate

## Problem and scope

R05 requires an implementation that can publish against a large existing graph,
as well as functional evidence. The current `ManagedCurrentGraphPublicationDriver`
loads all `whole_source_history` and all four data roles into the Python driver,
replays complete history through `plan_apply`, and embeds `baselineDataRows` and
`expectedRows` in a 4 MiB attempt plan. This makes graph size, rather than the
accepted page, determine driver memory and prevents a 1B-node/assumed 5B-edge
production claim. Its bounded functional path remains useful evidence but is
not an incremental production publisher. This design is a proposed replacement,
not a qualification or approval to run a scale trial.

## Exact incremental boundary

Public UMF and the selected installed Truss release still own model acceptance,
assigned IDs, typed values and complete source transactions. Retain the exact
accepted report/view/source and page bytes. For one bounded admitted page, stage
its complete ordered changes, raw event bytes/digests, delivery identities,
affected object/edge keys and referenced endpoint keys under the original
request/attempt identity. An oversized page refuses without truncation; page
chunking needs a separate durable protocol. Do not infer a new schema meaning,
source identity or SQL rewrite in the host adapter.

Require one qualified immutable predecessor manifest, exact source progress,
exclusive writer authority, its four data-role UUID/version heads and the
separate six-role installation schema/protocol inventory. The predecessor's
integrity coverage must have been established by bootstrap or an earlier
qualified publication; a narrow check cannot promote an unqualified baseline.
At those pinned versions, query only incoming delivery markers in
`whole_source_history`, affected current object/edge identities, their latest
tombstones and referenced endpoint identities. Match existing delivery bytes
and digests exactly; conflicting marker reuse refuses. Preserve absent, explicit
null, exact JSON tokens, source-qualified keys and independent parallel-edge
IDs. The complete raw history remains durable in Delta but is not replayed into
the driver per page.

Reduce the admitted page against that bounded affected state using the existing
portable apply semantics or a reviewed incremental equivalent. A final edge
requires both exact typed endpoints in the final page state or pinned predecessor.
For a node deletion, a native anti-join/existence check over predecessor edges
must prove there is no surviving incident edge after the page's edge removals;
it returns a scalar witness, never the hub's edge bag. Endpoint-pair grouping
cannot substitute for edge IDs. The native query may still scan broad files;
clustering and runtime cost need measurement, not an assumed O(page) claim.

Prepare a canonical bounded intent containing the original page/request digest,
predecessor manifest and four heads, six installation identities, touched-key
set, expected affected afterimages, raw-history delivery digests, exact effect
operations and expected source-progress transition. It must not contain the
complete baseline or complete expected graph. Apply source-qualified key MERGEs
and append exact raw-history/tombstone deliveries. Validate affected rows by
exact key and value at observed post-write versions, the distinct incoming
delivery set and its digests, the incident-edge scalar gate, and each role's
actual native head. Untouched rows rely on a validated predecessor and enforced
exclusive writer isolation; Delta DDL alone does not enforce uniqueness or
fencing. A zero-effect source page still commits source progress in a manifest.

Only then append the immutable manifest with four data-role versions, accepted
schema revision and the complete per-feed progress vector. Keep attempt and
manifest UUIDs/heads in the separate six-role inventory. Exact committed replay
reads the original intent and manifest without another data commit. An applying,
unknown or mismatched native outcome refuses and withholds ACK until original
operation recovery proves its result; matching current rows alone never permit
resubmission. The external source holder independently verifies the durable
destination receipt before protected Truss ACK. Old publication versions stay
readable under the selected retention boundary.

## Small implementation sequence and evidence

1. Add a versioned managed incremental planner/affected-key native read owner;
   keep `graph_sql_plan` and the complete-state managed path for their existing
   bounded evidence. Reuse `TrussGraphAdmission`, public UMF DDL, the existing
   attempt/manifest stores and source receipt owner. Do not change Weft output.
2. Switch a separately named managed publication profile to the bounded intent
   and affected-row/effect validation. Admit only an empty new target or a
   qualified predecessor, selected exact runtime/source authority and a held
   writer. Retain fail-closed uncertain apply until original-operation recovery
   has its own evidence.
3. Test the retained seven-change current page against an independently built
   final-state oracle; add two-source overlapping IDs, duplicate/conflicting
   delivery, absence versus null, parallel edges, missing endpoint and a
   high-degree node-delete existence witness. An inert transport must refuse
   unfiltered history/current `SELECT *` and show bounded collected rows as
   baseline size grows. One selected small native UC continuation then checks
   exact affected rows, four-vector, six inventory, replay and source ACK.

Those checks establish functional incremental behavior, not 1B/5B throughput,
freshness, retention or cost. After structural implementation, obtain the
owner's workload, thresholds and resource estimate for a separate dedicated
scale trial. Measure source-to-manifest/ACK latency, affected file/read bytes,
hot-hub and scattered-update behavior, sustained arrivals, reader latency and
retained storage. The current $100 functional trial ceiling does not authorize
full-scale materialization.
