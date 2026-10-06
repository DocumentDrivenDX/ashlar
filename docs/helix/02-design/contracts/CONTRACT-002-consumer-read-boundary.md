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
