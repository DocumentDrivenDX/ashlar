# Authored typed graph source on local Delta

Use [configured-typed-graph-source.json](configured-typed-graph-source.json) with
a separate fresh configured-local installation and a clean public UMF checkout
at `07357eadbda9cf8299b9d72d004cbb77f8091aee`. Its explicit
`ashlar-jsonl-typed-graph-source/0.1` configuration pins the original model, event
bytes and development bindings. Preserve all three inputs; do not relabel an
existing scalar installation or change its producer revision.

Follow the [installed configured-local workflow](../configured-local/README.md):
install the `local-delta` extra and select the Python, JDK, two Delta JARs,
Bun/Git and producer checkout explicitly. Initialize at ordinal 0 with fresh
installation/output paths. Retain the installation report and exact handoff SHA,
then publish ordinals 1, 2 and 3 with the same source, installation, stream and
materialization clock and a fresh output each time. Repeat the last completed
ordinal with a fresh output for exact replay. Use the same command for every
invocation:

```sh
python -m ashlar run-configured-local --configuration /absolute/operator/invocation.json
```

For an object singleton read, select a completed ordinal, `operation: "read"`,
the original installation/source pins, a fresh output and an explicit selector:

```json
"selector": { "type_id": 17, "entity_id": 1 }
```

Type `17` is Person and type `18` is Place. Both may use object ID `1`; the type
remains part of identity, so select `18` to read Place 1. Relationship type `31`
is knows (Person to Person) and `32` is located (Person to Place). Edge IDs are
independent identities; they must not collapse parallel edges or be used as
object singleton selectors. The 13 events across three complete groups include
parallel edges, a self-loop, an isolate, cross-type endpoints, endpoint
replacement and deletes. An older selected publication may retain an object or
endpoint that a later publication changes or deletes.

The host must freshly validate complete carried Record values, including delete
payloads, and jointly validate compact-dataset source keys and relationships
before planning native work. Retain the original public context, warnings and
receipts; do not convert warnings or supplied-dataset closure into broader
model/consumer authority. Edge properties must be the empty JSON object.

The host accepts at most 256 KiB per pinned reference, 64 KiB per admitted model,
four complete groups, 1000 events and 1 MiB per public validation request. The
core source configuration permits source bytes up to 1 MiB, while this host
route retains its lower 256 KiB reference ceiling.

For query mode, select the same completed ordinal and the explicit indexed Weft
compiler settings described in the configured-local guide. The admitted graph
subset covers required facetless STRING projections, global count, forward
bounded key collections, exactly-two-hop root projections/counts and required
STRING-root grouped path/distinct-terminal counts, for example:

```sql
SELECT p.person_key, RELATED_KEYS(p.knows, 2) AS related FROM Person p
SELECT p.person_key FROM Person p CROSS JOIN EXPAND_PATHS(p.knows, knows) AS paths
SELECT COUNT(*) AS n FROM Person p CROSS JOIN EXPAND_PATHS(p.knows, knows) AS paths
SELECT p.person_key, COUNT(*) AS paths, COUNT_DISTINCT_PATH_TARGETS(x) AS targets
FROM Person p CROSS JOIN EXPAND_PATHS(p.knows, located) AS x GROUP BY p.person_key
```

Key collections retain duplicate target keys from parallel edges and report
truncation. Two-hop root projections repeat a root once per independent path.
Select ordinal 1 to query the original topology after publishing ordinal 3.
Path counts include every independent edge pair; distinct counts use complete
terminal keys. Global empty expansions return zero; empty groups produce no rows.
Inverse traversal, optional/numeric keys, filtered graph queries and
direct edge-row projection require separate admission. Explicit development IDs
are not Truss catalog IDs, and local file progress is not a protected source ACK.
The cooperative local profile supplies no Unity Catalog, remote fencing or
production writer authority. Accept command completion only after its bounded
final result and all required closing checks succeed.
