---
ddx:
  id: ashlar.evidence.weft056-current-xxhash64-consumer-admission-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: FEAT-003
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
    - id: CONTRACT-004
      kind: governed_by
    - id: TD-007-native-xxhash64-lookup-layout
      kind: informed_by
    - id: TP-001
      kind: informed_by
    - id: ashlar.evidence.r01-upstream-pin-refresh-20261029
      kind: informed_by
---

# Weft 0.4.56 current `.4` consumer admission — 2026-10-29

Status: **effect-free path and refusal controls implemented; current `.4`
compiler response and native query remain unqualified**. This is a separately
versioned consumer route for the exact `ashlar-delta/0.4-xxhash64` physical
profile. It does not rebind the retained `.3` R02 packet, `.50` campaign, or
`.53` continuation.

## Release identity

The immutable public [`v0.4.56` release API](https://api.github.com/repos/DocumentDrivenDX/weft/releases/tags/v0.4.56)
identifies source commit
`2f62e1148d1b2030558550e4f6f2ffe518bac62d`, the macOS arm64 wheel at 11,190,603
bytes with SHA-256
`68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d`, and the
Linux x86_64 wheel at 14,358,901 bytes with SHA-256
`32f4c38d9a614dd4c3a367f8b24d5cd4ba41ec80c2a4a2e3423e2082a294dbdd`. The
macOS digest is also in `pyproject.toml`, the `.56` runtime index builder and
runtime verifier. An independently staged copy at
`/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/wheels/umf_weft-0.4.56-cp39-abi3-macosx_11_0_arm64.whl`
was hashed locally: 11,190,603 bytes and SHA-256
`68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d`, matching
the release API metadata. This verifies those staged wheel bytes; no claim is
made about any other cached copy.

The R01 pin table contained a typo in the macOS digest; it now matches the
official release API, package coordinate and code pin. The release source at
[`crates/weft-python/src/lib.rs`](https://github.com/DocumentDrivenDX/weft/blob/2f62e1148d1b2030558550e4f6f2ffe518bac62d/crates/weft-python/src/lib.rs)
exports the `compile_json` and engine page compiler/check APIs used by this
route. The `.56` integrity boundary now verifies and exposes `compile_json`
through the pinned runtime index.

## Admission boundary

`ashlar_host.managed_current_weft_056` accepts only a verified
`Weft056Distribution`, the current managed publication-held
`ManagedCurrentReadOwner`, the Backend03 `.4-xxhash64` target, and binding bytes
that identify the `.4-xxhash64` layout and its typed Spark hash profile. It
delegates compilation and execution to the existing `run_backend03` consumer
path. Callers must supply independently reviewed SHA-256 values for the
canonical request and exact compiler response. A narrow admission callback
checks the raw response digest, parsed `.56` response, backend and SQL fields
after compilation and before the native read boundary. That path sends the
compiler-produced artifact and SQL unchanged through
`read_weft` under its publication vector, schema pins, policy and closing
checks. The `.56` wrapper retains the exact compiler response bytes and hashes
the original request SQL and emitted compiler SQL for review. It does not
interpret or repair compiler SQL.

The current Weft `.4` Backend02 page path already admits the verified
`Weft056Distribution` through the same page target helper used for installed
Core/Access admission. That keeps the original page and continuation APIs on
the public `.56` runtime. Singleton lookup remains the publication resolver's
native read boundary under CONTRACT-004 and does not depend on Fabric Graph.

## Evidence and limits

Focused unit checks cover verified `.56` API exposure, exact current `.4`
Backend03 target/layout admission, retention of response bytes, and refusal of
a legacy `.3` carrier before the owner call. The tests use controlled compiler
and owner objects; they make no native calls.

No independently reviewed `.4` response digest exists yet, so this admission
route must refuse native execution until one is produced and reviewed. The
retained R02 SQL and compiler responses are bound to the earlier `.3`
publication. They are useful comparison inputs only. There is no current `.4`
publication vector or new `.4` compiler response in this receipt, and no `.4`
SQL was executed. Before native use, the exact `.4` vector must produce fresh
Backend03 grouped and Backend02 page/continuation requests and responses; an
independent review must confirm the generated lookup predicates and integrity
obligations preserve the source identities, bag multiplicity and complete
publication vector. Then each native result must match the independent
source-derived oracle with closing source/pin/runtime checks. The `.56`
distribution and this admission code alone qualify neither execution nor a
managed runtime.
