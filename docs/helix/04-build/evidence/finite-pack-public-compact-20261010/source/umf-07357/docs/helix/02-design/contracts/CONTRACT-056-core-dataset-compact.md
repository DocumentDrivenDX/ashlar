---
ddx:
  id: CONTRACT-056
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
    - id: FEAT-005
      kind: informed_by
    - id: CONTRACT-049
      kind: informed_by
    - id: CONTRACT-001
      kind: references
    - id: CONTRACT-040
      kind: references
    - id: umf.architecture
      kind: references
---

# CONTRACT-056: Compact supplied-dataset value receipts

**Type:** browser-compatible library interface. **Version:** experimental 1.0.0.

## Purpose

Retain complete finite dataset validation evidence with one shared original source
instead of repeating that source in every Record and Key receipt. Preserve the
meaning, warnings, ordering, scope and limits of the public logical value operations.

## Scope and Boundaries

UMF owns logical Record/Field/Key and supplied relationship validation. Consumers
own original wire-byte custody, CSV or graph-literal conversion, storage bindings,
source authority and native execution. Compact receipts MUST NOT imply accepted
catalog IDs, physical coverage, uniqueness enforcement, publication or source ACK.
The source remains valid declared core 0.8.0; implicit version conversion is forbidden.

## Normative Surface

`validateCoreDatasetValuesCompact(source, input)` MUST accept the existing
`CoreDatasetInput` request defined by CONTRACT-049 without additional execution
members. Its operation is `validate-core-dataset-values-compact`, version `1.0.0`,
with `scope:"supplied-dataset-only"` and `provenance:"unverified"`.

The result MUST retain exactly these top-level members:
`operation`, `version`, `scope`, `provenance`, `source`, `input`,
`documentValidation`, `records`, `keys`, `relationships`, `datasetValidation`,
`obligations`, `residuals`. Source and input MUST be copied original JSON data.
Unknown source and opaque input context MUST remain intact. JSON member ordering
has no semantic significance; every array order and literal spelling MUST remain.

The bodies and selected semantic checks MUST follow CONTRACT-049's supplied-dataset
operation, including complete individual Record diagnostics and separate dataset
context discharge. The compact operation MUST NOT call the full dataset operation
and then strip its result: exceeding the full repeated-source budget MUST NOT
prevent an independently bounded compact operation.

| Member | Required shape and rules |
| --- | --- |
| `records[]` | `{instanceId,result}` in original input order. `result` is the complete public `CoreRecordValueCheck`, except its `source` member is replaced by `sourceRef:"#/source"`. No other member may be omitted or changed. |
| `keys[]` | `{instanceId,result}` in original declared-key order. `result` is the complete public Key tuple v3 receipt except `source` is replaced by `sourceRef:"#/source"`. Original typed values, identity, keyPath and bytesHex remain exact. |
| `relationships[]` | Original occurrence order and complete instance/relationship/source/target identities. `targetKey` is the complete original Key tuple v3 receipt except `source` is replaced by `sourceRef:"#/source"`. Parallel occurrences remain distinct. |
| `sourceRef` | Required literal `"#/source"` at these three locations only. It references this receipt's own top-level source, never a registry, URL, external hash or another receipt. The corresponding `source` member MUST be absent. |
| Validation, obligations and residuals | Original results and complete source-qualified unresolved content. Unknown meaning MUST prevent completeness exactly as in CONTRACT-049. No diagnostic, extension content or residual may be pruned to fit a budget. |

A compact Record or Key fragment is not a standalone public receipt. A consumer
MAY reconstruct an individual original receipt by replacing its sole `sourceRef`
with the shared copied source and applying that public operation's verifier or
exact recomputation. No API promises expansion of an entire compact dataset into
one full receipt that exceeds the common JSON limits.

`verifyCoreDatasetValuesCompact(receipt, current, expectedInput)` MUST require
independently retained current source and expected input, recompute the complete
compact operation and compare the complete result as JSON data. It MUST reject
changed source, input, scope/context, locators, diagnostics, values, byte payloads,
array order, references, unknown members and original validation results. It MUST
NOT accept a digest, caller-supplied valid/complete flag or successful fragment
validation as a substitute for complete recomputation. Copy isolation and accessor,
prototype, cycle, sparse-array and interoperable-number checks remain mandatory.

### Bounded resource profile

The operation MUST retain the common limits of 128 levels, 100000 JSON values
and 4000000 text bytes; supplied arrays remain bounded to 1000 records and 10000
relationship occurrences. The aggregate compact receipt MUST contain at most
4000000 UTF-8 JSON bytes, including every Key value, encoded hex payload,
diagnostic and residual occurrence. Before composing each fragment, the operation
MUST enforce its individual public operation limits and charge its full retained
compact body against the aggregate value/byte budgets. Final accessor-safe copy
and aggregate byte checking MUST still run. No partial result may escape.

Semantic work MUST remain bounded to 1000000 conservatively charged JSON-value
visits. Compact representation alone MUST NOT discount semantic work. Composition
MUST use an invocation-local immutable validated source/input context. The original
source is copied and validated before any selected check; indexes and the original
documentValidation are derived only from that frozen source. Context MUST NOT be
supplied by callers, cached across calls, reused after mutation, or treated as a
new authority. Independently supplied source/input drive verifier recomputation
through a newly created context.

The implementation MUST share the original public Record/Field/Key semantic
evaluators internally, including presence, facets, Key equality, unsupported
meaning and diagnostics. It MUST NOT create a second semantic validator or call
the repeated-source public composition and then claim its work was avoided.
Public operation wrappers MAY retain their existing copy/revalidation behavior;
compact composition uses the shared evaluators with this fixed context and
constructs fragments directly. It MUST validate source meaning once per context
and retain that same original validation in every Record fragment.

Before dispatch, a conservative work ledger MUST charge every full source
copy/traversal/validation pass by the complete source value count, and every
selected semantic operation by at least its accessed source subtree value count.
Traversal, temporary copies, index construction, selected Field/Key checks,
unknown/residual inspection, aggregate accounting, output copying and verifier
recomputation MUST NOT be uncharged. The ledger MUST refuse before an operation
can exceed its ceiling. It MUST state the charged operation categories and
conservative counts in implementation tests, and prevent extra semantic helper
calls from silently exceeding the accounted plan. This is bounded library work,
not a wall-time, CPU or native performance guarantee.

An exact per-Record member/key inventory MAY tighten the preflight upper bound
compared with multiplying every row by an unrelated Record's maximum inventory.
Such tightening MUST reflect the actual shared evaluator calls and temporary
traversals. It MUST NOT justify using uncharged repeated-source public calls.
Unknown or non-Record identities MUST refuse before composition. Tests MUST
qualify the actual charged total for original archaeology/ecology and prove
resource refusal under the same ceiling. No positive test may pass by changing
these ceilings or pruning the original source/input.

## Precedence and Compatibility

CONTRACT-049 continues to govern literal, presence, field membership, Key equality,
relationship subset, multiplicity and unknown-meaning semantics. This contract
changes only shared-source receipt representation and explicitly bounded
composition. Existing `validateCoreDatasetValues`, `verifyCoreDatasetValues`,
Record and Key signatures, result schemas and resource guards MUST remain
unchanged. Neither receipt operation may masquerade as the other.

`spec/core/dataset-value-compact-operation.schema.json` MUST own the compact
request/result schema, exposed as `coreDatasetValueCompactOperationSchema`.
Required reference sites are closed operation-owned members; original source and
opaque context remain preserved JSON. Unsupported future receipt versions refuse.
No downgrade, model pruning, alias inference, default insertion, coercion or
native equivalence claim is permitted.

## Error Semantics

| Condition | Outcome | Recovery |
| --- | --- | --- |
| Invalid source, request identity or execution member | Existing source/input error; no receipt | Correct original explicit request. |
| Invalid logical values, duplicate Keys, missing/ambiguous target or multiplicity | Complete original diagnostics; `valid:false,complete:false` | Correct separately authored input; retain original failure. |
| Unsupported/unknown source or relationship meaning | Original residuals and incomplete result | Preserve meaning and supply a separately supported semantic extension. |
| Structural, fragment, aggregate or work budget exceeded | `LIMIT`; no partial receipt | Use a separately bounded operation or future explicit profile; never truncate. |
| Forged, stale, malformed or re-scoped compact receipt | `CORE_DATASET_COMPACT_RECEIPT` | Recompute from independent exact source/input. |

## Examples

```ts
const compact = validateCoreDatasetValuesCompact(originalSource, originalInput);
verifyCoreDatasetValuesCompact(compact, originalSource, originalInput);
// A Record fragment retains every original field result and warning.
const { sourceRef, ...body } = compact.records[0].result;
const originalRecordReceipt = { ...body, source: compact.source };
// sourceRef is exactly "#/source"; no external resolution is performed.
```

Acceptance MUST include original commerce and supply-chain parity with their full
receipts, original archaeology and ecology finite datasets under the unchanged
compact limits, present-null versus absent, exact decimal exponent spelling,
unknown extension/residual retention, duplicate/parallel occurrence handling,
forged reference/source/input/result refusal, copy isolation, accessor refusal,
aggregate bytes/values/semantic-work rejection, and byte-identical Bun/real Chromium
results. Full operation resource controls MUST remain unchanged. Actual native
storage or querying is separate consumer evidence.
