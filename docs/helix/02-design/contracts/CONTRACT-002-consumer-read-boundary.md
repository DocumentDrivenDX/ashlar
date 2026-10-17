---
ddx:
  id: CONTRACT-002
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: FEAT-003
    kind: informed_by
---

# Contract: consumer read boundary

**Contract ID**: CONTRACT-002
**Type**: boundary
**Version**: proposed ashlar-read/0.1
**Status**: Draft
**Related**: QUERY-01–QUERY-09; US-003; CONTRACT-001

## Purpose

Specify bounded read requests, progress, result completeness and failure outcomes
independently of query syntax, transport, physical layout and UMF bindings.
The normative surface is a proposed synthetic profile pending owner review.

### Truss STRING presence projections

`query-truss-graph` MUST preserve optional STRING values in direct, unfiltered,
unordered field projections from one admitted record scan. A required field
returns its exact string. An absent-allowed field returns either
`{"state":"absent"}` or `{"state":"value","value":STRING}` from the original
compiler's value representation. Native NULL, explicit null state, a foreign
Field descriptor or unsupported representation MUST refuse without a result.

The original accepted model, property allocations, compiler IR, output positions
and complete publication vector MUST correspond. Previously stored rows MUST
retain identical field definition pins and keys under an additively compatible
revision; a new absent-allowed field can remain absent on those rows. Every
emitted integrity guard MUST run before the original query. Compiler bindings
MUST explicitly cover the admitted physical revisions; the host MUST NOT repair
SQL or omit guards to admit older rows. The complete result bag, including older
compatible rows and multiplicities, MUST match the independent source-derived
projection before successful closing checks release it.

### Compatible physical-revision obligations

The `0.1.1-compatible-revisions-candidate` backend uses explicit
`compatibleSchemaRevisions` sets for physical record bindings. Ashlar MUST
establish original accepted catalog custody for each selected revision and prove
record ownership, property allocations, field definition pins, scalar domains,
ordered existing members and key correspondence against the primary revision.
New fields on older rows MUST be absent-allowed. Unknown revisions, required
additions, changed declarations or incompatible keys MUST refuse.

The emitted `ashlar.compatibleSchemaRevisions` host obligation MUST match the
original selected record sets, model pins and complete publication vector. It
provides no source authority or compiler proof of compatibility. The executor
MUST still discharge every original integrity obligation, retain the original
SQL and compare the complete source-derived result bag before releasing results.
A binding/proof API alone MUST NOT imply query execution qualification.

### Truss STRING equality join projections

A single INNER equality join MAY project up to 64 required STRING fields from
its two admitted record occurrences. The adapter MUST preserve matching pair
multiplicity, validate exact accepted homes and original compiler descriptors,
and compare the complete result bag under the same full publication pins and
scalar guards. The independent oracle MUST index equality values and refuse
more than 10,000 matched pairs before materializing them. Filters, grouping,
ordering, limits and relationship-edge joins require separate admission.

Compatible-revision STRING projections MUST authenticate both original scan
bindings and selected physical revisions. Interpret only consumed required,
facetless STRING equality and output Fields. Unconsumed mathematical integers,
decimals and unknown extension content MUST retain their original lexical/model
bytes without imposing a new numeric execution domain. Equality MUST preserve
Unicode code points without normalization; this admission MUST NOT broaden
numeric joins, mixed numeric outputs or STRING count support.

### Truss STRING equality join count

A single INNER equality join over two required STRING record occurrences MAY
produce global `COUNT(*)`. The adapter MUST validate both accepted property homes
and original typed compiler IR under the complete publication vector, execute
all original scalar guards and unchanged compiler SQL, and compare the exact
nonnegative count with the complete source frequency-product oracle. Equal values
MUST retain pair multiplicity; this operation MUST NOT imply relationship-edge
join semantics. Filters, grouping, ordering and limits require separate admission.

### Truss relationship existence direction

A `HAS_RELATED` predicate MAY use an explicitly authored inverse name and MUST
require `relationship.inverse` exactly for that direction. Query endpoint roles
and key tuples MUST reverse together while canonical physical edge mapping,
multiplicity and lifecycle remain unchanged. Matching edges MUST select each
source record once, including parallel matches. Both directions MUST retain
all integrity guards and compare the complete publication-bound result bag.

### Truss related-key direction

A `RELATED_KEYS` read MAY traverse an explicitly authored inverse name. It MUST
retain the canonical physical edge mapping and require the compiler's
`relationship.inverse` capability exactly when an inverse traversal is used.
Query endpoint roles and authored key tuples MUST reverse together; original
relationship multiplicity and lifecycle declarations MUST remain unchanged.
Parallel edges MUST retain separate occurrences, including equal key tuples.
Both directions MUST apply the same integrity guards, deterministic prefix
ordering, truncation and complete publication-bound result checks.

### Original supply-chain two-hop composition

`query_supply_chain_two_hop(driver, pack, aliases, config, *, observe)` MUST consume the
actual finite supply-chain driver under its existing installation/source lease.
It MUST retain the original UMF 0.8 model, graph, source receipt, binding and
complete publication vector. Caller-supplied contexts, source validators or
schema validators MUST NOT substitute for the owning public interfaces.

`SupplyChainTwoHopQueryConfig` MUST explicitly select `distribution`
(`PathsDistributionPaths`), `maximum_artifact_bytes` (positive integer, at most
16 MiB), `capture` (`PathCaptureConfig`) and `decoder` (`PathDecodeConfig`).
No ambient compiler, profile fallback or implicit capacity is permitted. The
composition MUST acquire the selected installed schema bundle and derive its
offline admission and finite public-source checker internally.

`observe(ordinal, iteration, request_bytes, response_bytes)` is a required
protocol-evidence callback. Ordinals are zero through eleven; iterations are zero
and one. Both byte views MUST satisfy the selected artifact bound before the
callback runs. This callback confers no source, schema or publication authority;
callback failure MUST withhold the combined result and preserve closing checks.

All twelve compatible directed relationship pairs MUST execute through the
shared held-path executor, including source rows with no paths. Original and
independently recompiled protocol artifacts MUST agree before execution. Exact
authored endpoint keys and independent edge occurrence identities MUST match
the complete original oracle; unexpected duplicate occurrences, reordered,
truncated or malformed answers MUST NOT qualify as complete. Parallel edges
with equal endpoints MUST remain distinct by edge identity. Every query MUST retain the same full
publication identity/vector and renew original source/native custody.

Results remain provisional until the outer owner independently closes the
reader, source, installation and native session. A failed query or closing check
MUST withhold the complete combined result. This composition creates no source,
publication, compiler installation or ACK authority. It qualifies only the
two-hop case; the other six common graph cases retain their separate required
positive executions.

### Existing supply-chain publication reads

`open_supply_chain_common_read(config)` MUST yield only a nominal
`OpenedSupplyChainCommonRead` bound to an existing original publication, source,
installation lease and read-only journal. Missing registrations, committed
phases, native tables or exclusion records MUST refuse without creation or
repair. Reopening MUST NOT publish, append, submit ACK, regenerate the dataset
or create a replacement receipt. Existing native invocation profiles retain
their own behavior and evidence inventories.

The read composition MUST select a nominal `RetainedOriginalSupplyChainDataset`
as the `FiniteSupplyChainSource` producer at construction and every renewal.
Its acquisition MUST independently verify the authentic original successful
public-producer invocation and exact original model, graph, selected producer
source/runtime/script and emitted receipt correspondence. Caller callbacks,
receipt digests and archived validation flags MUST NOT supply semantic authority.
Missing original provenance MUST refuse. Complete original dataset, record,
key and relationship semantics MUST remain checked.

A current source invocation MAY validate the original corpus independently before
opening the reader. Its complete emitted receipt bytes and source-admission
metadata MUST match the existing publication exactly; a mismatch MUST refuse.
This establishes current source correspondence, not provenance of the historical
publication-producing invocation or durable restart restoration. Reader
construction and renewal MUST NOT execute that validation or replace its receipt.

`OriginalSupplyChainDatasetInvocation` MUST be minted only after an actual
successful public-producer invocation and its independent original closing
checks. A same-process retained owner MUST accept only that live origin.
Durable reopening MUST use a separately admitted origin store; deserializing
caller JSON, digests or validation flags MUST NOT reconstruct authentic origin.
Selected executable correspondence MUST stream complete bytes under fixed
ceilings of 256 MiB for Bun and 32 MiB for Git. These checks rely on genuine
operator-selected tools and the trusted host; byte equality MUST NOT be treated
as tool authentication or transitive runtime qualification.

Renewal MUST verify held original file identities and complete model, graph,
script and receipt bytes, plus the selected producer's original revision and
clean source. Necessary bounded read-only Git HEAD/status operations MUST run
against the original selected paths and environment; fresh dataset execution
MUST NOT substitute for these checks. Changed configuration MUST NOT redirect
original closing checks. Producer provenance and semantic correspondence MUST
NOT grant current native, source, publication or ACK authority.

Every query-only interval MUST acquire an existing exclusion record and bind
its original descriptor and complete table UUID/version vector. On normal
return or any `BaseException`, including cancellation, it MUST independently
attempt original source renewal, descriptor/vector resolution and native/alias
correspondence before releasing the actual exclusion record. Closing failure
MUST withhold the result; an original body failure MUST be preserved while all
applicable original closing checks and releases are attempted. Nested provider
intervals and resolution of a replacement publication MUST refuse.

`OpenedSupplyChainCommonRead.query_provider(context, *, family, part, ordinal)` MUST
construct an inactive query reader and detached request from the fixed original
corpus selection. It MUST accept no supplied provider, binding or authority
callback. The executor MUST acquire that reader's genuine nonnested interval;
the common owner MUST register it only during the actual held interval and
independently remove it after closing. An active flag alone MUST NOT grant
current admission. Successful closing MUST retain the actual interval receipt.

### Installed supply-chain native invocation

`ashlar run-supply-chain-native --configuration PATH` MUST accept a closed,
explicit JSON configuration of at most 65,536 bytes. Profile
`ashlar-original-supply-chain-native-invocation/0.1` retains its existing fields
and five authored query results; it MUST NOT accept `two_hop`. Profile
`ashlar-original-supply-chain-native-invocation/0.2` MUST require the same fields
plus a nonnull `two_hop` object. No profile fallback is permitted.
Profile `ashlar-original-supply-chain-native-invocation/0.3` MUST require the
same fields as 0.2 plus `compiler_evidence_format`, whose sole accepted value is
`binary-protocol/0.1`. It MUST execute the same complete five authored and twelve
two-hop queries and retain the same result shape. Profiles 0.1 and 0.2 MUST NOT
accept this additional field.

The base fields are exactly `profile`, `output`, `jars`, `pack`, `model`,
`graph`, `producer`, `compiler` and `operation_capacity`. The five path fields
select the fresh output directory, local Delta JAR directory and original pack,
model and graph files, respectively; all MUST be absolute nonsymlink paths.
`producer` contains exactly `source`, `bun`, `git` (explicit absolute source
checkout and executable paths), `timeout_seconds` (integer 1–60),
`maximum_output_bytes` (integer 1–1,048,576) and `maximum_receipt_bytes`
(exactly 8,388,608 for this complete original-dataset profile). `compiler`
contains exactly `index` and `installation`, explicit absolute nonsymlink paths
to the installed count compiler selection. `operation_capacity` is either null
for the existing base capacity or exactly
`{"profile":"ashlar-private-local-operation-capacity/0.1","max_intent_bytes":8388608}`.
Integer tokens MUST be canonical nonnegative decimal tokens of at most eight
digits; fractional and nonfinite tokens MUST refuse. Every path string MUST be
nonempty and at most 4096 characters; configuration supplies no compiler package
or ambient path discovery.

The `two_hop` object MUST contain exactly `distribution`,
`maximum_artifact_bytes`, `capture` and `decoder`. `distribution` contains exactly
`index` and `installation`, each a canonical absolute nonsymlink path to the
explicit installed Paths compiler selection. `capture` contains exactly
`maximum_rows` (integer 1–128), `maximum_cell_bytes` and
`maximum_total_cell_bytes` (integers 1–16 MiB, cell no greater than total).
`decoder` contains exactly `maximum_cell_bytes` (integer 1–16 MiB).
`maximum_artifact_bytes` is an integer 1–16 MiB. Duplicate keys, unknown members,
missing values, booleans used as integers, nonfinite numbers and incompatible
path aliases MUST refuse before acquiring runtime or creating output.
Output and configuration custody MUST remain disjoint from both selected
compiler indexes/installations and the original source inputs.

The native composition MUST execute the five authored queries followed by all
twelve two-hop queries through the same restored read-only driver, original
pack, aliases, publication vector and source/installation lease. The five
`cases` retain their existing shape. Profile 0.2 additionally returns complete
`two_hop` evidence and `two_hop_queries` equal to twelve; profile 0.1 retains its
existing result shape. A new publication, substituted driver or raw SQL fallback
MUST NOT stand in for this composition.

For profiles 0.2 and 0.3, the compiler evidence sink MUST retain ten count-protocol
and twenty-four Paths protocol records under one unchanged 8 MiB aggregate bound.
Profile 0.1 MUST retain its ten count-protocol records under that same bound.
Each request and
response byte view MUST satisfy both its selected artifact bound and the sink's
1 MiB per-view bound before observation. Profiles 0.1 and 0.2 count hex encoding
and complete record bytes toward the aggregate bound.

Profile 0.3 MUST retain the original request and response bytes in separate
exclusive binary files and retain a complete bounded inventory index. Its
unchanged 8 MiB aggregate MUST count all original binary bytes and all serialized
index bytes. Each index record MUST bind the known case or query ordinal,
iteration, two deterministic relative filenames, lengths, SHA-256 digests and
held file identities. Filenames MUST derive solely from the five original case
identities or twelve ordinals and iterations zero and one. Caller paths,
symlinks, overwrites, extra files and implicit rotation MUST refuse.

The sink MUST preflight the complete pair and index record against remaining
capacity before creating files. Complete-byte writes and independent original
content, identity and directory-inventory closing MUST succeed before a record
is complete. A partial write or index failure MUST withhold the combined report.
The complete 0.3 inventory MUST contain exactly thirty-four records and
sixty-eight binary files. It MUST preserve every original byte without
normalization, recompression or truncation. Exported reports MUST reference the
bounded inventory rather than expand its binary files into hex. Retention
capacity does not qualify query execution; actual compiler artifacts, native
results and the independent oracle remain required.

Native evidence and the final report each
have a 64 MiB complete-byte bound. Exceeding a bound MUST withhold the combined
report, without truncation or a successful prefix. Query, observer, cancellation
and closing failures MUST preserve the primary failure while independently
attempting original source, reader, transport, compiler, lease, runtime/session
and configuration closing checks. Results become complete only after all those
checks succeed. These invocation profiles create no Truss, ACK or remote-engine
authority.

## Scope and Boundaries

Ashlar owns the read outcome and observable publication boundary. The execution
adapter owns proof that an authenticated caller's effective policy applies.
Fixtures simulate policy; they do not prove end-user delegation on Databricks.

## Normative Surface

| Element | Type / Shape | Required | Rules |
| --- | --- | --- | --- |
| Request | `{operation, args, limit, workLimit, waitMs, minimumProgress, context, cursor?}` | yes except cursor | No unbounded requests |
| operation | lookup, list, traverse, count, history, changes | yes | Only shapes below |
| limit | integer 1–100 | yes | Maximum returned rows/groups/paths |
| workLimit | positive integer | yes | Profile-declared maximum candidate items examined; exceeding gives incomplete, never exact success |
| waitMs | integer 0–30000 | yes | Maximum wait for required progress; no implicit infinite wait |
| minimumProgress | array of CONTRACT-001 progress entries | yes | Empty allows latest complete publication |
| context | `{principal, policyRevision}` | yes | Authenticated principal and effective policy revision; caller strings alone do not authenticate |
| cursor | opaque string | optional | Bound to request, publication and authorization context |
| Response | `{status, code?, rows, complete, publication?, progress, cursor?}` | yes except optional members | rows array; progress vector; no fabricated boundary |
| status | ok, incomplete, not_ready, denied, unavailable, unsupported, recovery_required | yes | Distinct execution and refusal outcomes |
| complete | boolean | yes | true only for a completed authorized answer at the declared boundary |

Lookup/list rows are `{identity, version, value}` entity records. Traverse nodes
rows are identity tuples; traverse paths rows are `{nodes: identity[], edges:
identity[]}`. Count rows are `{group: string or null, count: nonnegative integer}`
with `{missing: true, count}` for missing groups. History/changes rows are
CONTRACT-001 history entries. Response publication is a publication ID string.

A successful read MUST observe one immutable complete publication. All emitted
rows MUST follow the same effective row and property policy. Filtering and count
inputs MUST use that policy; hidden rows and properties MUST NOT influence visible
counts, filters or traversal. Edges with hidden endpoints are excluded in this
profile. Unsupported protected-property queries return denied uniformly,
without confirming hidden entity existence. Policy changes invalidate cursors.

| Operation | args | Result semantics |
| --- | --- | --- |
| lookup | `{identity}` | Zero or one visible entity; absent or policy-hidden entity yields ok/empty |
| list | `{type, equals: object}` | Visible nodes of type matching exact property equality; hidden filter properties denied |
| traverse | `{start, steps, mode}` | steps array of 1 or 2 `{edgeType, direction}`; direction out/in; mode nodes/paths |
| count | `{type, groupProperty, related?}` | related optional `{edgeType, direction, groupProperty}`; group visible starts or related endpoints |
| history | `{identity, version}` | Exact retained visible version; expired evidence returns recovery_required |
| changes | `{feed, epoch, after}` | Accepted logical history strictly after the position, through chosen publication |

Traverse mode nodes returns distinct terminal identities. Paths returns distinct
edge-identity sequences including their node sequence. Revisited nodes are allowed
within the requested fixed depth. No expansion beyond two steps occurs. Explicit
edge identities preserve parallel paths; a source profile excluding them MUST
reject incompatible input before read execution.

For count without related, count distinct visible start identities per property
value. With related, count distinct visible start identities per visible related
property value, avoiding parallel-edge multiplication. Missing properties form a
separate missing group; explicit null forms a null group. No coercion of values.

Entity ordering is lexicographic by kind, type, key; path ordering by edge sequence.
Counts order by missing first, null second, then string group values; the synthetic
count profile supports only missing/null/string groups. History orders by version;
changes by source position, then identity. Orders are case-sensitive. For this
fixture profile all identifiers and sortable strings are ASCII; locale or Unicode
collation is a future profile choice.

Result caps yield incomplete with complete false and a continuation cursor if
safe continuation is supported, otherwise code RESULT_LIMIT. Count execution MUST
finish computing exact groups before pagination; a work cap produces incomplete
with code WORK_LIMIT and no count rows. Any work cap returns no rows and no cursor
in this profile. A continuation fixes the publication and policy context. If that
publication expires or context changes, return recovery_required/CURSOR_INVALID;
never continue against a newer snapshot silently.

Minimum progress is compared only within matching feed/epoch. If a feed is known
but not caught up before waitMs, return not_ready/PROGRESS_BEHIND, empty rows and
current observable progress. Unknown feeds or mismatched epochs return
recovery_required/PROGRESS_INCOMPARABLE. Authorization precedes progress disclosure.
If no safe complete publication is available, return unavailable without a claimed
publication. Mixed table progress cannot produce status ok under this profile.

## Precedence and Compatibility

CONTRACT-001 governs publication and retained history. This Contract governs the
read outcome. Denial is evaluated before executing a query or exposing progress.
Store failure after authorization is unavailable; unsupported delegation is
unsupported/DELEGATION_UNSUPPORTED. It MUST NOT fall back to a service principal.
Count, paging, policy and progress changes require a new profile revision.

Retention lower bound is the earliest supported after-position, inclusive.
A changes request below it is recovery_required/HISTORY_EXPIRED, not an empty
complete answer. Historical policy in this profile uses the current effective
policy at read time; historical row/column evaluation needs separate proof.

## Error Semantics

| Condition | Outcome / code | Retry / recovery |
| --- | --- | --- |
| Invalid bound or unsupported shape | unsupported/INVALID_REQUEST | Correct request |
| Caller not allowed operation/property | denied/NOT_PERMITTED | Authorized context required |
| Delegation unavailable | unsupported/DELEGATION_UNSUPPORTED | Implement supported delegation |
| Execution unavailable | unavailable/STORE_UNAVAILABLE | Bounded retry; no success claim |
| Source behind requested position | not_ready/PROGRESS_BEHIND | Retry after publication |
| Unknown feed or epoch | recovery_required/PROGRESS_INCOMPARABLE | Reconcile positions |
| Expired cursor or changed context | recovery_required/CURSOR_INVALID | Restart read |
| Expired history | recovery_required/HISTORY_EXPIRED | Bootstrap/reconcile |
| Row/group cap | incomplete/RESULT_LIMIT | Continue if cursor exists |
| Work cap | incomplete/WORK_LIMIT | Revise workload or budget |

Failures MUST have empty rows and complete false; result-limit pages alone may
carry rows. Authorized empty results have status ok and complete true. Denied
responses omit publication and expose an empty progress vector in this profile.

## Examples

At publication pub2 with progress S/e/2, lookup isolated TypeA/A0 returns A0 and
complete true. A two-hop nodes request from A1 returns C1/C2 once each. The same
request with two distinguishable A1→B1 edges in paths mode returns both paths.
Requiring S/e/3 while gold remains at 2 and waitMs is zero returns not_ready,
empty rows and observed progress 2. An unauthorized lookup returns denied without
indicating whether the requested identity exists.

## Validation Checklist

- [ ] Owner reviews count, paging, wait and disclosure proposals (PRD Q11).
- [ ] Corpus includes exact successful and refused read outcomes.
- [ ] Adapter proves effective user policy across all operations.
- [ ] Platform evidence establishes immutable publication reads and continuation.

## Non-Normative Notes

No HTTP endpoints, generated API, authentication provider or query compiler is
selected. UMF and physical type compatibility remain later integration gates.

### Supply-chain grouped two-hop occurrences

An explicitly selected `two-hop-count` case MUST count original independent
two-edge paths through `items.lot_id` then `lots.product_id`, grouped by the
authored item ID. It MUST use the public compiler's `CROSS JOIN EXPAND_PATHS`
and `COUNT(*)` operation; property joins and host counts MUST NOT substitute.
The original UMF version, binding, edge-pair witness bag and publication vector
MUST remain exact. CROSS expansion returns only roots with paths; an absent
root does not represent an emitted zero count. This case MUST NOT qualify
one-hop degree counts, isolate coverage or the complete two-hop collection.
Existing default query selections MUST remain unchanged.

### Authored two-hop root rows

An explicitly selected authored two-hop root projection MUST execute the public
indexed compiler's original `CROSS JOIN EXPAND_PATHS` artifact. The initial
profile admits one required facetless STRING root-key field, without filters,
ordering, limits or aggregation, on one admitted physical revision. Each original
ordered edge-occurrence pair emits one root row; parallel edges and reuse of a
self-loop in both positions retain multiplicity, and isolates emit no row.
Complete independent source/key/home correspondence, original scalar,
relationship and occurrence guards, recompilation and full publication closing
MUST precede release of the exact result bag. This profile does not qualify
destination fields, general joins or aggregate/count artifacts.

### One-publication query batches

A bounded query batch MUST select one to eight uniquely named original query
intents and one immutable publication. Each intent MUST explicitly select SQL,
model revision, document ID and row limit. The ordinary source, installation and
compiler settings remain explicit. The batch MAY share one owned native runtime,
but each query MUST retain its complete source admission, original compiler
artifacts, integrity checks and native pins. Every result MUST match the original
publication ID and complete manifest; mixed refresh MUST refuse.

Unknown configuration members, duplicate names and exceeded input, row or report
bounds MUST refuse without truncation. A failed query, changed batch input or
failed outer closure MUST withhold the combined success report. Final persistence
MUST follow source, transport and runtime closure and original input renewal.
Sharing a runtime MUST NOT permit publication, source ACK or new source authority.

A typed batch MUST explicitly select each query family and its compiler transport.
Compatible scalar projections and numeric grouped counts MUST retain their
physical revision selections; indexed two-hop counts MUST retain their independent
recompiled bytes. Compiler configuration MUST cover exactly the selected families,
so an unused compiler cannot block another family's execution. Dispatch MUST reuse
the family's admitted executor without SQL repair or weaker obligations. All
original protocol bytes, including recompilation where required, count toward
the aggregate bound and MUST remain buffered until the entire batch closes.

### Declared-field record reads

A named-field view MUST derive ordered field identities and native property IDs
from the record row's admitted catalog revision. It MUST retain the complete
original row alongside the view and preserve absent, null and present value
states without inferring a default. A missing row MUST remain explicit absence.
Unknown executable property IDs or incompatible scalar carriers MUST refuse.
The view MUST use the existing singleton's publication, source and native checks;
it MUST NOT broaden the admitted scalar or nullability profile. Final reports
MUST be bounded and follow owned resource closure.

### Typed Truss compiler bindings

A typed scalar compile request MUST retain the exact accepted UMF document and
its declared facets, actual property allocations and complete publication vector.
It MUST NOT invent numeric bounds or rewrite an original source version. The
STRING-only request API MUST retain its closed subset. An unsupported declared
scalar meaning MUST produce the original compiler refusal; compiling a binding
MUST NOT imply native execution or discharge host obligations.

A direct required-field scalar projection MUST bind each original Field descriptor
to its accepted property allocation and declared scalar facets. Integer and decimal
results MUST retain exact compiler carrier tokens; complete result comparison MUST
use exact mathematical values without float conversion or rounding. Scalar admission
MUST remain explicit and MUST NOT broaden STRING-only commands, permit aggregates
without their separate admission, or bypass publication and integrity obligations.

Direct required decimal projections admit explicitly declared precision 18 with
scale 2 or 4. The original facets MUST match compiler descriptors and type graphs;
exact source and result values MUST be representable without rounding. This
selection MUST NOT broaden integer domains, filters, joins, grouping or arithmetic.

A required signed64 equality filter MUST bind the original source Field, exact
literal token, type and parameter origin. Its independent oracle MUST validate
the complete admitted source population before filtering and projection; filtering
MUST NOT reduce the scalar-integrity guard scope. The ordered, limited equality
subset MUST project only that same Field and order it ascending, so tied matches
have identical projected values. SQL limits MUST be positive and fit the selected
row bound. Other filter/order combinations require separate admission.

### Required numeric equality joins

A required numeric equality INNER JOIN MUST authenticate both original scan
bindings, each selected accepted physical revision and the exact declared scalar
domain of each join Field. A separate admission MUST preserve all original
publication and integrity guards. The complete result bag MUST match independently
joined source occurrences, including duplicate matches and self-join pairs, with
exact mathematical numeric comparison and original result carrier tokens. A join
admission MUST NOT authorize arithmetic, aggregation or optional values.

### Required scalar arithmetic projections

Arithmetic projections MUST retain the accepted model, declared source facets,
actual property allocations and full publication vector. They MUST use the
explicit indexed arithmetic compiler and its original artifact admission, then
execute every source integrity and arithmetic phase guard in the emitted order
before running the unchanged result SQL. Independent source comparison MUST use
exact arithmetic without float conversion or ambient decimal rounding. Result
carrier tokens MUST remain original. A single-revision adapter MUST refuse rows
from other physical revisions rather than silently filter them.

### Original commerce queries on a Truss publication

`query-truss-commerce` MUST select exactly one original pack case: `partial-return`,
`fulfillment`, `settlement` or `refund`. Required inputs are `--configuration`,
`--output`, `--weft-index`, `--weft-installation`, `--pack`, `--revision` and
`--document-id`; `--maximum-rows` defaults to 1000 and MUST be within 1–10000.
The command MUST retain the original pack SQL, accepted UMF bytes, declared facets,
actual property homes and complete held publication vector. Each record binding
MUST select its true homogeneous physical revision; mixed revisions within one
record require separate compatibility admission. Unconsumed relationship mappings
MUST NOT imply physical relationship compatibility.

Every consumed mathematical Integer Field MUST have its complete six-STRING
source projection compared with original accepted source rows before finite
representability or arithmetic checks. Source checks MUST be independent of the
result row bound. Exact numeric meaning and each interface's original tokens
MUST remain distinguishable. Guard failures MUST prevent result SQL execution.
Results MUST preserve column positions, including repeated names, and occurrence
multiplicity; authored ordering MUST be preserved. Independent comparison MUST
follow native capacity checks. Reads MUST preserve the source checkpoint and
release owned source, Spark and publication holds before successful report output.
A failed invocation MUST preserve its primary error and original publication;
query recovery MUST NOT republish or acknowledge source data.

### Runnable scalar arithmetic workflow

A local arithmetic example MUST accept explicit existing Bun, JDK, Delta JAR and
indexed compiler paths and create a fresh private output directory. It MUST use
public Truss acceptance and mutation APIs, publish the original source pages,
then perform an exact singleton and arithmetic query on that same publication.
Reads MUST leave the source checkpoint unchanged. Final success MUST follow
owned source and Spark closure, environment restoration and original configuration
renewal. The example MUST retain the original declared numeric facets and expose
its saved reader configuration for later commands.

### Numeric equality join counts

An admitted numeric equality INNER JOIN COUNT MUST count every matched source
pair, including duplicate matches and self-join pairs. Empty matches MUST produce
one zero count. The original count descriptor, both record identities and complete
publication and revision obligations MUST correspond. Count admission MUST retain
all original source and integrity guards and compare the complete single result
with independently matched source populations. It MUST NOT materialize or truncate
pairs to fit a projection row limit, or authorize other aggregates implicitly.

### Required numeric grouped counts

An explicitly selected numeric grouping query admits one required signed64 or
one required decimal(18,4) field, projected with one `COUNT(*)`, from an original
accepted record. The complete publication, model bytes, property allocations,
selected physical revisions and original compiler type/column metadata remain
bound throughout execution. Source scalar-integrity guards run before unchanged
compiler SQL. Group equality uses exact mathematical numeric values; counts
preserve duplicate source-row multiplicity and stay within nonnegative signed64
bounds. Numeric grouping carriers remain exact tokens. The complete unordered
result bag must equal independent grouping of the admitted source bag; an empty
source produces no groups. Missing groups, extra groups, incorrect counts,
unknown obligations and unavailable closing custody refuse buffered publication.
This admission covers unfiltered, unordered single-record grouping. Joins,
optional grouping fields, arithmetic grouping and other aggregates require their
own explicit admission.

### Authored key traversal over typed records

A separately selected current-revision `RELATED_KEYS` query MAY traverse an
accepted authored relationship whose records also contain integer, decimal or
Boolean fields. The exact original typed model, accepted field allocations and
relationship definition MUST remain in the compiler binding. Authored endpoint
keys and projected scalar fields in this route MUST be required facetless STRING
fields. Forward and inverse traversal retain canonical edge identities,
multiplicity, lifecycle and key tuple order. The complete publication is held
while every original scalar and relationship guard executes before unchanged
compiler SQL; buffered key bags MUST match independent original edge occurrences
and pass closing custody checks. This route MUST require a related-key output
and refuse relationship filters, historical revision unions, numeric outputs,
aggregates or paths without their own admission. Unreferenced numeric properties
remain exact during source comparison.

### Authored two-hop expansion counts over typed Truss records

An explicitly selected two-hop count route MUST bind the original accepted typed
model, exact current catalog revision, actual relationship definitions and their
canonical edge homes. Each hop MUST retain its authored required STRING endpoint
keys, direction, multiplicity and independent lifecycle. The logical path MUST
be contiguous from the selected source record. A global `COUNT(*)` over that
expansion MUST count original edge-identity pairs, preserving parallel occurrences,
self-loops and reuse of the same edge at distinct hop positions. Empty path bags
produce one exact zero count; the result stays within nonnegative signed64 bounds.

The original installed 0.4.2 compiler request and independent recompilation MUST
correspond exactly. All publication, scalar-integrity, relationship-integrity,
path-occurrence and count-capacity obligations MUST be admitted and discharged
under the same complete native publication hold. Source guards precede dependent
path guards; unchanged compiler SQL runs only after every guard succeeds. Per-hop
pinned edge schemas MUST preserve BIGINT identities. The complete buffered count
MUST match independent original node/edge populations and pass closing source and
manifest custody. Unknown obligations, missing endpoints, stale physical revisions
or incomplete evidence refuse publication. Filters, grouped path counts, property
joins and path-list outputs require their own explicit admission.

### Distinct authored two-hop terminal counts

An explicitly selected `COUNT_DISTINCT_PATH_TARGETS` route MUST preserve the
original compiler's terminal record, ordered authored key tuple and path occurrence
in its `pathTarget` column descriptor. The exact contiguous two-hop admission,
current revision and all five publication/source/path obligations remain required.
The independent oracle MUST count distinct terminal authored key tuples reachable
from admitted starts; it MUST NOT substitute path-occurrence multiplicity or
deduplicate by an invented identifier. Parallel paths and self-loops reaching the
same key contribute one target, while different keys contribute separately.
Empty reachable target sets produce one zero count. Exact required STRING key
values and nonnegative signed64 counts MUST remain publication-bound. The
original descriptor remains unchanged; only a locally derived, validated scalar
representation MAY be passed to the count decoder. Unknown or mismatched terminal
metadata, incomplete result bags and any failed source/path guard refuse buffered
publication. Distinct targets and ordinary edge-pair counts MUST remain separate
explicit query modes.


### Original mathematical-integer source guards

A required original UMF0.8 integer with no authored facets MUST retain its
mathematical domain and original exact-integer descriptor. The held Truss scalar
route MAY execute its compiler's finite DECIMAL38 candidate carrier only after
all original scalar-integrity checks succeed, including a nominal source guard
and its paired representability guard. No signed64 facet may be invented.

The source guard MUST authenticate the original model/field/record/property home
and complete admitted physical revisions. Its six STRING columns MUST match the
entire source population's typed identities, original property bytes and exact
integer tokens before user SQL, independently of the output row limit. Sign/zero
normalization is comparison-only; original source and compiler bytes remain
retained. Source captures MUST remain bounded to 10,000 rows, 65,536 bytes per
cell and 1 MiB total. Overflow MUST refuse rather than truncate. The original
`WFT-CAPABILITY` representability check MUST return zero and remain distinct from
source-domain failure. Unknown guard meanings or missing/mismatched pairs MUST
refuse. Existing signed64 predicate support is not broadened by this bridge.

### Saved private graph-export singleton

`read-graph-export` MUST select the existing closed four-field local export
configuration and one canonical reversible node or edge graph key. It MUST admit
both original artifact digests and complete carrier inventories under the actual
local file-use owner, with opening and closing checks before report availability.
The report MUST retain publication identity, selected release/custody digests and
the exact STRING/null carrier, or an explicit missing result. It MUST NOT promote
opaque property text into scalar values or collapse independently identified edges.

This operation uses `ashlar-local-development-private-export-use/0.1` from
CONTRACT-003, retaining its two-file 16 MiB bounds and all profile restrictions.
It reads a saved artifact and its selected private custody receipt.
It MUST NOT claim a live publication read, authenticated Unity Catalog access,
current source permissions, source fencing, protected ACK or engine qualification.
No SDK or source connection is required. Failed custody, malformed identity,
cancellation or failed closing MUST withhold report availability; a fresh owned
report is published only after complete successful file-use closure. The report
parent MUST be canonical, owned and private (mode `0700`); staged bytes and
original file identity MUST agree before and after no-clobber publication.
Cleanup MUST remove only owned report inodes, preserving foreign replacements.

### Original medical singleton query

The original historical medical singleton workflow MUST read the selected canonical
publication through the actual medical read-source factory, original UMF 0.8
admission and complete held four-role vector. The fixed query selects the required
`encounters.resource_key` equal to `Encounter/example`; its complete positional
result bag MUST match an independent scan of the original graph. This narrow
operation MUST NOT grant arbitrary medical query or action authority.

The operator MUST explicitly select the authenticated indexed f36/0.4.2 local
Spark 4/Delta 4 compiler installation. Both original compilation responses MUST
match, pass the pinned public schemas and artifact admission, and retain unchanged
SQL and ordered parameters. Every original integrity guard MUST execute before
the query. Fresh session-local external aliases MUST bind the same original native
UUIDs, locations and pinned versions; no source data, journal or ACK mutation is
permitted. Full native schema/source/ACK opening and closing observations, reader
and Spark closing MUST complete before any buffered result is published. A
compiler-only result or Databricks-qualified profile MUST NOT imply this local
execution passed.

The singleton receipt MAY retain the interval's repeated, fully validated ACK
bytes by exact byte lengths and SHA256 references to the outer read receipt's
full original ACK record. This report-only sharing MUST NOT replace or omit any
opening/closing observation or whole-byte comparison, truncate values, or enlarge
the selected complete receipt bound.

Exact compiler request and response bytes MAY be retained once in private
exclusive files under the read output after all owner closings. The receipt MUST
pin each file's exact byte length and SHA256, including the byte-equal independent
recompilation reference, and renew these original files before receipt publication.
This representation MUST preserve whole original protocols without duplicating
large payloads in the bounded aggregate receipt.

### Finite original medical full-row and two-hop queries

A closed medical query batch MAY select full-row singleton and complete two-hop
cases within one actual medical reader, source interval, Spark session and full
publication hold. Full-row singleton MUST project every original encounters Field
and reconstruct its declared typed key and complete original row. The earlier
required-key-only query MUST retain its narrower qualification.

The two-hop case MUST cover all five compatible original relationship pairs and
compare the complete 20-occurrence `(source, edge1, middle, edge2, target)` bag.
Original edge identities, repeated occurrences and declared key tuples MUST remain
intact; bounded RELATED_PATHS output MUST refuse true truncation. Each statement
MUST use the original indexed f36/0.4.2 compilation/recompilation, public schema
admission and unchanged emitted SQL, parameters, descriptors and obligations.
Full native/source/ACK opening and closing cover the whole batch. Per-statement
renewal MUST use the same original graph-read port and exact held vector, not a
cached permission. Every result stays buffered until all batch and outer closings.

Complete original protocols and per-statement evidence MAY be retained once in
exclusive private files after all closings, each bounded by the selected input or
receipt capacity as applicable and pinned/renewed before the unchanged 1MiB outer
receipt. No truncation, guard omission, capacity increase or qualification of the
unselected medical corpus cases follows from this finite batch.

An explicit `filtered-limit` selection MUST project the complete original
encounters row, order by the original required STRING key and apply unchanged
compiler-emitted `LIMIT 2`. Its independent witness MUST retain the complete
original population, key order and selected full rows. The original population
contains one encounter, so this case MUST NOT claim a binding-limit or truncation
qualification. Default batch selection remains full-row singleton and two-hop.

### Installed original medical query command

`ashlar query-original-medical --configuration PATH` MUST use the existing
supervised installed medical runner's closed `query` operation. Its configuration
profile `ashlar-original-medical-query-batch/0.1` has exactly `profile`, `read`,
`compiler` and `cases`. `read` is a whole existing original medical read
configuration. `compiler` has exactly absolute canonical `index` and
`installation` paths for the existing independently indexed f36 distribution;
paths MUST NOT choose trust pins, install packages or select fallback compilers.
`cases` is an explicit nonempty unique array validated by `MedicalQueryBatch`,
selecting `singleton`, `two-hop`, `filtered-limit`, `all-objects`, or their unique
combinations.

Parent and worker MUST retain the complete invocation selection and verify the
original indexed compiler at entry and final closing, including equal original
ready bytes. Each compilation MUST retain its independent original distribution
checks. The parent MUST hold actual runtime custody and enforce the configured
worker phase, native/whole deadlines, capture capacities and owned group cleanup.
The worker MUST isolate both Python stdout and native descriptor-one diagnostics
from the bounded JSON result. The public batch SDK MUST remain the owner of one
reader/Spark/full-vector interval, fresh statement renewals, complete bags and
closing. The SDK MAY retain private evidence and its receipt after its own
source/native/Spark closing. If later worker or parent configuration, compiler or
capture closing fails, those files MAY remain but MUST NOT establish command
completion. A CLI pass requires the bounded command result after every worker and
parent closing and successful supervised termination; drift MUST withhold that
result.
The command grants no source writes, ACK advancement or new publication authority.

### Complete original medical object projection

The finite batch MAY explicitly select `all-objects`. That selection MUST contain
all eight original record projections and compare the complete 51-object bag,
including isolated objects, original lexical keys, full declared member values,
and present-null states. Partial record inventories MUST refuse. Defaults remain
unchanged. The original model and SQL MUST remain unchanged. Explicit native-null
property encoding MAY be selected only after proving every original declared
member key is present; a missing native property MUST refuse, not become null.
Boolean native values MUST correspond to the original public Boolean receipts;
the result MUST retain correspondence to original lexical tokens.

Unfaceted mathematical integer outputs MUST use the actual `MedicalAdmission`
and its exact held public dataset, original source binding and acquired complete
publication vector. Before any representability, dependent arithmetic or user
statement, the owning source port MUST compare the complete six-STRING source
projection against independently reconstructed original properties and tokens.
The original field/property identities, mathematical domain, source population,
and paired compiler guard inventory MUST correspond exactly. Native DECIMAL38
representability remains a separate finite execution condition; it MUST NOT add
a signed64 facet or normalize original source values. All original statement,
batch and outer source/native/Spark closing checks and existing capacities remain
mandatory. Per-case evidence MUST retain exact original protocols after closing;
this finite selection grants no broader query or source authority.

### Installed managed-publication singleton

The named development profile `ashlar-managed-singleton/0.1` exposes
`read-managed-singleton --configuration PATH` for one existing, last-completed
configured-JSONL publication using `ashlar-delta/0.3`. It MUST preserve the
original installation, intake, source configuration/model/bytes, completed
ordinal, request, descriptor and complete registered four-table vector. It MUST
NOT ingest, replay, create an installation, register/release pins, advance source
progress/ACKs, or substitute current table heads. CSV, evolution, arbitrary SQL,
compiler and graph-export operations are outside this profile.

The dedicated read warehouse MAY differ from the original writer warehouse.
The original warehouse MUST remain unchanged in retained request and clock
evidence; a read MUST NOT rewrite or replay those submissions. The reader MUST
independently admit its selected endpoint and actual actor, original table
identities/versions, source correspondence, retention and complete native pins.
Changing read compute grants no publication, migration or source authority.

The closed configuration MUST select pinned original installation, intake-proof
and source-configuration files (each `path` and `sha256`), the existing `journal`
path, exact `publication_id` and `completed_ordinal` (1–4), explicit
`source_system`, signed64 integer `type_id` and `entity_id`, authenticated
Databricks `profile`/`host` and dedicated `warehouse_id`, explicit public UMF
producer paths/bounds, the pin-reader object below, finite statement/whole/output
bounds and a fresh private `output`. Unknown fields and inferred selectors MUST
refuse. Existing journal/WAL state MUST be read consistently without schema
initialization or mutation. The original cooperating-writer exclusion MUST remain
held throughout selection, native consumption and closing.

The concrete owner MUST renew the actual SDK actor and current catalog/schema/
table owner-lane effective permissions, original native intake row, MANAGED table
identities, protocol, complete ordered schemas, retention and independently
source-bound full snapshot parity. It MUST use the public resolver and singleton
under a real `PostgresPins.hold` covering the complete original vector. Expected
identities in configuration and retained validation flags are comparisons, not
permission grants. A matching row MUST retain every canonical carrier; an absent
row MUST return explicit null after the same admission/closing checks. Results
remain provisional until pin, native, source/input, transaction and supervisor
closing succeeds. Missing prior artifacts or uncertain journals MUST refuse,
without reset or repair.

#### Explicit ordinary pin-reader prerequisite

`pins` is a closed `ashlar-managed-pin-reader/0.1` object:

```json
{
  "profile": "ashlar-managed-pin-reader/0.1",
  "host": "127.0.0.1",
  "hostaddr": "127.0.0.1",
  "port": 25440,
  "expected_server_address": "192.0.2.10",
  "expected_server_port": 5432,
  "dbname": "operator_pin_database",
  "user": "operator_pin_login",
  "sslmode": "disable",
  "passfile": "/absolute/operator/private/pins.pgpass",
  "connect_timeout_seconds": 5,
  "statement_timeout_ms": 5000,
  "lock_timeout_ms": 1000
}
```

This initial connection profile is same-host loopback only: both address fields
MUST equal `127.0.0.1`; `port` is an exact integer 1024–65535.
`expected_server_address` MUST be an explicit single numeric IPv4/IPv6 address
and `expected_server_port` an exact integer 1–65535, independently selected from
the original pin-service observations; neither is inferred from the client
endpoint. The example server address is illustrative, not a working selection.
`dbname` and `user`
MUST match `[a-z][a-z0-9_]{0,62}`. No multi-host, URI, service, options,
password, provider import or callable configuration is accepted. Exact positive
bounds are connect 2–5 seconds, statement 1–15000 ms and lock 1–5000 ms with lock
not exceeding statement. The concrete psycopg3 owner MUST pass these selections
explicitly, disable GSS encryption negotiation, and select fixed session options
for the statement/lock bounds. It MUST refuse any inherited `PG*` setting before
connection rather than modifying process-global environment or permitting libpq
service/default selection. The `passfile` MUST be an explicitly selected,
canonical, current-user-owned regular non-symlink file with mode 0600 and at most
64 KiB; its identity MUST be checked before and after acquisition. Its content,
content digest, DSN, connection exceptions and notices MUST NOT be retained or
rendered. The passfile is a credential reference, not evidence of authority.
The profile assumes cooperating operator-owned credential files and does not
claim hostile-filesystem or remote PostgreSQL authentication protection.

The connection MUST authenticate directly as the selected existing ordinary
LOGIN role: observed `current_user` and `session_user` MUST both equal `user`,
`current_database()` MUST equal `dbname`, and native `inet_server_addr()` /
`inet_server_port()` MUST match the distinct expected server address/port.
Forwarded client ports and loopback addresses MUST NOT be equated with native
server identity; Docker may expose a loopback port different from its server
port/address. Address text MUST compare as numeric IP values, not DNS labels. Superuser, role/database creation,
replication and row-security bypass privileges MUST refuse. Required pin-reader
function/inventory access MUST exist; pin registration/release, maintenance and
direct pin-table write privileges MUST be absent. No role switching, Docker
credential discovery, role creation or grants are permitted. The existing DDL's
NOLOGIN `ashlar_pin_reader` role is not a usable login merely because its name
appears in configuration; an already provisioned ordinary LOGIN with the required
reader privileges is an operator prerequisite.

Each acquisition MUST return a fresh non-autocommit idle connection. The
transaction adapter MUST keep that exact connection and its native locks across
context yield, commit normal completion, roll back failure and always close.
Cleanup MUST preserve original primary exceptions and the first non-Exception
cancellation when no prior operation cancellation exists. Ordinary driver failures
MUST become fixed sanitized connection refusals. A finite supervised whole-run
bound remains mandatory; libpq connect timeout and SQL/lock timeouts are not a
complete wall-clock guarantee. The pin database has no invented installation-ID
field: installation correspondence MUST instead retain the original managed
installation/descriptor and exact complete registered `PinVector` under
CONTRACT-004. Neither loopback login nor matching pins establish Unity Catalog,
UMF Subject/action/disclosure, remote writer-fencing or source ACK authority.

## Configured local immutable-file singleton reads

The isolated configured-source development profile MAY read a selected completed
publication under a continuously held cooperative installation lease. It MUST
bind the original installation registry, source configuration and complete source
bytes to the selected ordinal's retained local progress proof and five committed
attempt phases. Missing proof tables or phases MUST refuse; read mode MUST NOT
create, publish, replay, repair or advance progress.

The read MUST resolve the exact complete UUID/version vector, admit current native
schema/protocol and finite retention, and preserve full original row fields.
Independent complete source-prefix carrier checks and closing source, progress,
phase and lease checks MUST complete before releasing either a row or absence.
Unknown types MUST refuse; an absent ID within an admitted type MAY return null.
Later updates or deletes MUST NOT redirect an older selection to current heads.
Local lease custody MUST NOT be represented as PostgreSQL pins, remote writer
fencing or protected producer ACK authority.

## Configured local Weft queries

`run-configured-local` MAY select `operation: "query"` on an existing completed
ordinal using the same original source, installation and full-vector read custody.
Its required closed `query` object has `profile: "ashlar-configured-local-weft/0.1"`,
`sql`, `compiler` (`index`, `installation` absolute paths) and `bounds`
(`maximum_artifact_bytes`, `maximum_rows`, `maximum_cell_bytes`,
`maximum_total_cell_bytes`). Positive limits MUST NOT exceed 1 MiB for artifacts
and total cell bytes, 1,000 rows or 65,536 bytes per cell. Settings and compiler
selection MUST remain explicit; a configured path is not evidence of authority.

This profile admits one original record scan with required, facetless STRING field
projections, preserving positions and repetitions, or a single global `COUNT(*)`.
Filters, joins, groups, ordering, limits, optional values and other scalar families
require separate admission. The exact original compiled plan and property homes
MUST correspond to the original public UMF source and complete publication.
Independent source-prefix rows MUST supply the expected result bag; generated
SQL or observed native results MUST NOT supply it.

Both indexed compilations MUST match exactly and pass the pinned public schemas
and host artifact admission. Fresh external aliases MUST preserve native Delta
UUIDs, locations and pinned versions without relabeling the manifest. Every
emitted guard and the original SQL MUST execute unchanged. Original protocol
texts and bounded results MUST remain buffered until alias, publication, source,
native runtime and supervisor closing succeeds. Query mode MUST NOT initialize,
publish, replay, repair or advance source progress. This local development
qualification grants no Unity Catalog, protected ACK or remote fencing authority.
