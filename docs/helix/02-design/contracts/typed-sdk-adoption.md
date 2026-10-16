---
ddx:
  id: ashlar.typed-sdk-adoption
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-002
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
---

# Typed SDK publication read adoption

**Contract ID:** ashlar.typed-sdk-adoption. **Type:** library.
**Version:** `ashlar.typed-sdk/0.1`. **Status:** implementation candidate.

## Purpose

Implement UMF CONTRACT-069/072 read targets over Ashlar's original publication
resolver, pin interval and Weft execution. The SDK receives no write capability.

## Scope and Boundaries

Catalogs retain complete original `weft.engine-read/0.1` programs. Weft owns
compiler correspondence. Ashlar owns native publication admission, current
result authorization, exact scalar decoding and the complete pin interval.
Temporal qualification remains HELD. No target-created session, publication,
policy, credential or fallback snapshot is permitted.

## Normative Surface

`AshlarTarget` is constructed from the host's executor, resolver backend, pins,
original vector, policy, context and supported profile/revision inventories.
`identity()` includes the exact publication/vector in its digest context.
`read(entry,parameters,timeout=...)` requires a source-current `EngineRead` and
passes the original request/program to the Weft comparison before execution.
The complete original artifact is executed only through `read_weft` under its
held resolver interval. Native absence for a ColumnSet-bound Field refuses as
ADOPTION_MISMATCH. Complete decoded rows are revalidated through generated DTOs
before release. Denials, expiry and native errors never become empty results.

Read-time bindings may change declared parameter values only. The host policy
continues to bind exact compiler artifacts, source identities, native profile,
publication, current caller and complete results. Custody comparison confers no
authority. Every callback and native operation remains within one finite deadline.

## Precedence and Compatibility

Publication and read contracts override this adapter. A new engine/compiler,
model generation, accepted binding or publication requires requalification.
Continuation identity includes publication and vector; a changed or expired
publication raises StaleContinuation. The first concrete adapter must refuse
unsupported page/export programs rather than fabricate continuation semantics.

## Error Semantics

Unknown program/profile, mismatched source/binding, absent bound values or expired
snapshots refuse. Existing resolver/native errors propagate. No retries occur.

## Examples

A host supplies an authorized held publication and a generated `OrderSummary`
catalog. A read decodes inside that original pin interval. Replacing a native
table UUID or resolving another publication refuses release or continuation.

## Validation Checklist

Actual local Delta E2Es compare generated reads with the original resolver path;
negative controls cover expired/replaced snapshots, changed aliases, current
result denial, source mismatch and wrong generated carriers. Compiler-only and
callback-only tests do not qualify native adoption.
