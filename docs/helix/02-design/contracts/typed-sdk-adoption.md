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

Catalogs retain complete original `weft.engine-read/0.1` or
`weft.engine-page/0.1` programs. Weft owns
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
Continuation identity includes publication, vector, complete original program,
parameters and bounded opaque owner context; changed or expired custody refuses.
Pages require the distinct `ashlar-spark4-pages` or
`ashlar-dbsql-20261016-pages` compiler registration, interface 0.2.2 and its
matching candidate backend version. Historical read profiles remain unchanged.
The original authored Key and its integrity obligation must be admitted by the
host, checked natively before data SQL, and retained in the original program.
The host supplies `hold_page`, `admit_page_integrity` and `page_context`; missing
capabilities refuse. Opaque context is immutable bounded bytes, not authority.

Every read and page freshly resolves the original authoritative manifest and
all UUID/version/schema identities after fetch, before buffered results escape.
Closing authorization and owner context must still agree. Snapshot streams
advance through the same immutable publication with explicit row bounds; each
page releases its held resources before yielding. Reaching `max_rows` without
original proof of exhaustion raises the SDK's `StreamLimitReached` after the
emitted prefix; it never appears as successful exhaustion. A buffered remainder
sets `truncated=True`; an exact page boundary with a continuation sets
`truncated=None`. Closing the iterator releases its current page and does not
fetch another page. Empty pages with continuations refuse as non-progress.

The optional `page_size` is positive and cannot exceed the original authored
page bound. A smaller size is compiled through the original engine page API from
the exact original request, source/navigation packet and result model. The
adapter retains the original opaque catalog context and rechecks every derived
page through the original checker and held host admission. It never edits SQL,
lookahead metadata or compiler digests. A missing original page compiler refuses
as `STREAM_PAGE_SIZE_UNSUPPORTED`. Timeout remains per page. Async adoption
remains open.

## Explicit serialized async bridge candidate

`ashlar.async_access.SerializedAsyncTarget` is a host-owned serialized thread
bridge under `ashlar-serialized-native-cancel/0.1`. It is not a native-awaitable
Spark driver claim. An immutable `SerializedReadHost` supplies the original target
identity and scope token, a worker-only target context, a worker native absolute
deadline context, a nonblocking/no-database-I/O caller scope check, and a separate
nonblocking native cancellation controller callable outside that worker. The
adapter creates no database connection. Missing contracts refuse before dispatch.

Queue wait, target entry, compilation and execution consume one absolute read or
page deadline. The worker recomputes remaining time after target entry. Zero is
an already expired deadline; negative or foreign timeout values are invalid.
Parameter models are reconstructed and continuations copied before queueing;
the original synchronous target rechecks original catalog and native authority.
Caller context is checked before dispatch and before every public row release,
with a copied context on the dedicated worker. No previous worker actor supplies
caller authorization.

Each async stream page opens, executes and completely closes its host scope on
the worker before yielding any buffered row. The exact original smaller-page
entry remains in continuation custody across pages. There is no native prefetch
while the consumer holds a yielded row, and closing the async iterator fetches
nothing further. Per-page timeout and `max_rows` semantics remain those above.
A denied closing context withholds the whole page.

Observed task cancellation always propagates after worker settlement and cleanup,
including cancellation racing a completed native result. Repeated cancellation
does not detach the worker. Deadline cancellation uses the same controller and
waits for settlement; ordinary/native cleanup failures remain exception causes.
An operation may already have completed natively; it is never replayed. A provider
that cannot actually cancel and settle is not qualified by these source controls.

Source lifecycle controls are retained separately from native qualification:
[Async bridge source controls](../../04-build/evidence/typed-sdk-async-source-20261017/README.md).
Actual in-flight native cancellation, event-loop progress, original pin/actor
custody and denied native closing remain required before async adoption claims.

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

## Execution evidence

[Local page qualification](../../04-build/evidence/typed-sdk-pages-20261016/README.md)
records 17 actual Spark 4.0.1/Delta 4.0.0 and PostgreSQL 18.6 controls. A real
alias change after fetch first exposed a buffered-result defect and then refused
after fresh closing resolution. This qualifies the explicit local STRING fixture
and trusted fixture policy only. The separate managed packet passes 11 independent-reader controls on the exact
2026.39 DBSQL build. Production policy enrollment, broader carriers and async
adoption remain open. Temporal qualification is HELD.
