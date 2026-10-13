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
