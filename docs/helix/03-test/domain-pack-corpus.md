---
ddx:
  id: ashlar.domain-pack-corpus
  type: test-suites
  activity: test
  status: draft
  authoring:
    home: repo
  links:
  - id: TP-001
    kind: informed_by
---

# Domain-pack corpus

## Suite inventory

Use [domain-pack-corpus.json](fixtures/domain-pack-corpus.json) as the finite
case index for P02/P08 in the [toolkit plan](../04-build/toolkit-delivery-plan.md).
It fixes 24 pack inputs, 180 case IDs and 151 engine case IDs. IDs identify test
contracts; execution receipts belong under build evidence. An engine case ID is
`ENGINE/{engine}/{case.id}` using the explicit `engineCases` lists. Do not expand
these lists into an unrestricted pack × engine × query matrix.

| Case prefix | Required behavior | Primary layer |
| --- | --- | --- |
| `PACK/{pack}/PIN` | Check every original file/source hash and preserve notices; report declared external inputs separately | Fixture custody |
| `PACK/{pack}/MODEL` | Invoke public UMF document reading/validation on original owning-version bytes; preserve diagnostics and source | Public interface |
| `PACK/{pack}/DATA` | Bind the original bundled rows explicitly and invoke public Field, Record, Key and supplied-dataset operations where applicable | Public interface |
| `PACK/{pack}/GRAPH` | Admit the exact candidate with its own original model, keys, typed endpoints and occurrence bag | Public interface |
| `QUERY/{representative}/{query}` | Execute against a complete immutable publication and compare full results to the independent source oracle | Native integration |
| `SCENARIO/{representative}/{scenario}` | Answer the named original source question without running pack/compiler SQL as the oracle | Native integration |
| `AUGMENT/{fixture}` | Exercise separately authored scalar, presence and topology boundaries | Contract and native integration |
| `REFUSE/{condition}` | Reject the named adversarial mutation at its specified boundary | Contract |
| `PUBLICATION/{transition}` | Prove complete state, history, progress and original-operation recovery | Native integration |

Every pack receives PIN, MODEL and DATA cases; the 16 authored graph candidates
also receive GRAPH cases. All five representatives require successful data and
graph admission. Optional constructs in other packs may receive a source-qualified
refusal with retained input and exact residual meaning. Such a classification
does not satisfy a required representative, publication or engine pass.

## Test data

The JSON index contains each pack's exact original model, graph, table, source,
notice and asset file hashes and byte lengths. Its pinned inventory digest binds
all source declarations, including references without available bytes. A source
with `sha256: null` has no observed bytes; do not synthesize a digest, fetch it
implicitly or count it as ingested. Runtime receipts must name the exact public
UMF, Weft, engine, source-profile and model versions they actually use.

`current-pack` and `historical-pack` describe original input provenance. Legal and
medical graph candidates belong to pack 1.0.0 while their current models belong
to 1.1.0. Their archive digests and every archive member hash are fixed in the
index. Read the historical model from `schemas/ontology.json` and preserve the
archive's original source data. Never pair either historical graph with its
current model or rewrite its declared pack hash.

| Pack | Current model | Original graph objects / edges | Graph input |
| --- | --- | --- | --- |
| archaeology | Core 0.8 | 39 / 46 | Current |
| commerce | Core 0.8 | 11 / 10 | Current |
| construction | Core 0.8 | 20 / 27 | Current |
| cybersecurity | Core 0.8 | 18 / 22 | Current |
| ecology | Core 0.8 | 40 / 54 | Current |
| education | Core 0.8 | 13 / 16 | Current |
| energy | Core 0.8 | 42 / 43 | Current |
| gtfs-schedule | No authored ontology | None | Source declarations only |
| hr | Core 0.8 | 11 / 13 | Current |
| legal | Core 0.8 | 232 / 405 | Historical 1.0.0 |
| manufacturing | Core 0.8 | 15 / 19 | Current |
| martech | Core 0.8 | 16 / 23 | Current |
| medical-carrier | Core 0.8 | None | Current bundled tables/sources |
| medical-epidemiology | Core 0.8 | None | Current bundled tables/sources |
| medical-imaging | Core 0.8 | None | Current bundled tables/sources |
| medical-terminology | Core 0.8 | None | Current bundled tables/sources |
| medical | Core 0.8 | 51 / 62 | Historical 1.0.0 |
| movielens | No authored ontology | None | Source declarations only |
| noaa-ghcn-daily | No authored ontology | None | Source declarations only |
| nyc-tlc | No authored ontology | None | Source declarations only |
| payments | Core 0.8 | 13 / 14 | Current |
| real-estate | Core 0.8 | 18 / 20 | Current |
| supply-chain | Core 0.8 | 20 / 23 | Current |
| transit | Core 0.8 | 10 / 10 | Current |

Absence of an authored ontology or graph is an input fact. It requires an explicit
binding/model fixture before the corresponding semantic or runtime claim; it is
not evidence that a data format or domain is unsupported. Twenty valid document
declarations do not establish twenty valid datasets.

## Representative selection

| Order | Representative | Distinct meaning and mandatory questions |
| --- | --- | --- |
| 1 | commerce | Exact mathematical integer quantities and fixed-decimal prices, payment/refund arithmetic, typed product/supplier relationships and bags. Preserve all four authored scenarios: partial-return, fulfillment, settlement and refund. |
| 2 | supply-chain | Split lot reaches two shipments; two events retain the same upstream event ID; nested containers and absent parent; temperature 17.00 exceeds 8.00 with unit/method retained. Run all five original scenarios. |
| 3 | archaeology | Both contradictory stratigraphic assertions survive; overlapping dated interpretations retain both authors/evidence; specimen counts have separate denominators; external AS6 remains external. Run all eight original scenarios. |
| 4 | ecology | Censored O2 has threshold 0.10 and no measured value; known-effort zero OC2 differs from effort-unknown OC3; directional reach linkage, units, fractions and taxonomy ranks remain explicit. Run all nine original scenarios. |
| 5 | medical | Preserve all 17 current original resource JSON values, all 17 reference rows and boolean carriers. Birth/effective-date and value_decimal fields retain their authored string types. Run the historical graph separately with its own model and data. No clinical inference or terminological equivalence is introduced. |

Commerce, the other four representatives and the shared scalar, valid-presence and topology fixtures run
through Weft. GraphFrames runs each representative's seven common graph queries,
the four commerce scenarios and the shared scalar, valid-presence and topology fixtures. PuppyGraph Cypher,
PuppyGraph Gremlin and Fabric Graph GQL each run the same commerce and topology
cases, plus the shared exact scalar and valid-presence fixtures. Each lane records capability refusals independently; unavailable access
cannot replace an actual required engine pass or delay the other lanes.

## Independent oracle conditions

The operation names and oracle IDs in the JSON index have these meanings:

- **O-BYTES / O-DOCUMENT:** compare original bytes with the fixed digests, then
  preserve the public operation's source, validity, completeness and diagnostics.
  A missing authored ontology returns the explicit input classification.
- **O-RECORDS:** parse original CSV cells as text, retain row/column/source
  coordinates and lexical tokens, and apply an explicit source convention before
  constructing public value carriers. Preserve native null, logical absence and
  explicit null separately. Enumerate declared key components and call public
  key/dataset APIs; do not infer an identity from the first column or storage ID.
- **O-GRAPH:** independently read original objects and edge occurrences, compare
  each full typed key/value row, and resolve target keys with the public dataset
  API. Compare both occurrence bags and distinct endpoint sets. A candidate's
  `key` is an original locator, not an accepted native catalog identity.
- **O-ALL-OBJECTS / O-ALL-EDGES:** compare complete row bags, including original
  keys, field states, exact values, edge keys/types/direction and endpoint keys.
  Include isolated objects. Counts alone cannot satisfy either case.
- **O-ONE-HOP / O-TWO-HOP:** enumerate directed original edge occurrences in
  Python or another independent host implementation. One-hop rows contain
  `(source, edge, target)`; two-hop rows contain `(source, edge1, middle, edge2,
  target)`. Compare complete bags, allowing repeated nodes/edges as dictated by
  the declared walk. Never collapse parallel paths into a destination set.
- **O-GROUPED-COUNT:** independently group the one-hop bag by full source identity
  and relationship type; report both occurrence count and distinct destinations.
  Add a zero group for every isolated source when the query declares outer groups.
- **O-SINGLETON / O-FILTERED-LIMIT:** select the lexicographically first original
  object key, verify its full row, then select its original record type, order by
  complete original key and take at most two rows. Assert the declared limit and
  original total independently; no silent truncation is allowed.
- **O-SCENARIO:** resolve the fixed `manifestPointer`, read original CSV files,
  and independently compute the authored expected bag without executing its SQL.
  Use integers, decimal coefficients/scales or rationals, never binary floats.
  Apply only the authored identity-remapping rules when comparing graph results.
  Preserve both the source expected rows and the independent calculation.
- **O-MEDICAL-RAW / O-MEDICAL-REF / O-MEDICAL-TYPES:** compare original resource
  files to retained resource JSON meaning while preserving their original bytes;
  resolve each reference's source, native reference and JSON pointer without
  inventing targets; compare true/false as booleans and JSON/date/decimal-text
  fields as the exact original strings declared by their selected model.
- **O-AUGMENT / O-REFUSAL:** use the explicit fixture values and adversarial
  changes in the index. Author and pin their own public UMF declarations before
  execution. A topology fixture has five nodes, six independent edges, an isolate,
  a parallel pair, a cycle and a self-loop; its A1 two-hop bag has eight paths and
  four distinct destinations. Compare both. The presence fixture requires distinct
  valid absent, explicit-null, empty-string, zero and false states under separately
  authored, pinned public declarations that permit them. All five states must pass
  each engine; invalid excluded states are separate refusal controls. Unknown
  source content must retain its original bytes even when its semantics are refused.
  Unsupported temporal/variable-scale
  declarations receive an exact capability/semantic refusal, never a coercion.
- **O-PUBLICATION:** compare complete current/history/tombstone rows to original
  events after every named transition; verify the complete version/UUID vector,
  retained R1 rows, global predecessor and each source's epoch/progress. Inject
  interruption at the stated boundary and inspect visibility before recovery.
  A durable ACK requires the actual protected source receipt and original
  request/manifest bytes; a local checkpoint cannot substitute for it.

## Coverage mapping

PIN/MODEL/DATA/GRAPH and scalar refusals cover FR-1/FR-2 under TP-001.
QUERY/SCENARIO/engine and topology cases cover FR-3. PUBLICATION cases cover FR-4.
Require 100% of the finite case IDs to have a scoped outcome and 100% of required
positive cases to pass before claiming their capability. Preserve the consumer
plan's policy, disclosure, continuation and retention corpus alongside this suite.

Commerce quantity, amount, relationship, join, traversal and grouped-count
requirements remain positive acceptance conditions. A backend that cannot
represent a valid unbounded integer value must report a backend limitation and
retain its original logical domain; adding an int64 facet is forbidden. Synthetic
safe-integer boundary cases must pass exactly for their admitted profiles.

## Execution commands

Run public model admission with an explicit clean validator, original pack Git
repository and fresh output:

```sh
bun tools/check_domain_pack_models.ts "$UMF_VALIDATOR" "$UMF_PACKS" "$FRESH_OUTPUT"
PYTHONPATH=src:tools python3 -m unittest discover -s tests -p 'test_domain_pack_model_validation.py'
```

For each behavioral case, the implementing test must expose the exact case ID,
consume its fixed input hashes and emit a separate receipt. The index defines
future runner inputs; these document/custody commands alone execute no dataset,
publication or engine case. Use Python unittest for adapter checks and the public
upstream runtime for UMF/Weft checks. Native runners take explicit local profiles
or dedicated authorized endpoints; shared/default Databricks compute is excluded.

## Ownership and evidence

The pack adapter owner implements source/value cases; the publisher owner owns
mutation/recovery cases; the query/engine owner implements its explicit case list.
A separate reviewer checks the independent oracle and native receipts before
landing each slice. Record original inputs, public receipts, emitted SQL and
parameters, query rows, native versions/UUIDs, full release lineage and loss reports
under build evidence. Keep execution history and pass/fail status out of this index.

Each engine must also prove R1/R2 refresh: partial or failed successor import
exposes no mixed vector; R1 readers retain exact original rows or refuse explicitly;
activation and rollback select one whole release. Retention, authorization and
source checks must close before result release. Real Truss and managed Unity
Catalog claims retain their own admission and native execution requirements.
