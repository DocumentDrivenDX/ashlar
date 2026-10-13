# Compatible accepted physical revisions — isolated proposal

Base: Weft clean commit 94b2de5. Original dirty checkout untouched.

Add `records[].compatibleSchemaRevisions` to the existing public Ashlar binding.
`schemaRevision` remains required and is the primary selected revision. Omission
retains exactly the legacy equality guard. Explicit selections must contain
1..32 unique nonempty strings (each <=256 UTF-8 bytes), include the primary, and
apply only to Object/Edge physical records. Null/empty/duplicates/missing primary
refuse. Projection rows have no physical schema-revision carrier and refuse this
selection.

The existing scalar-integrity obligation becomes a parenthesized OR of exact
UTF8_BINARY parameterized equalities. Any unselected row revision is still a
pre-query violation; no rows are silently filtered and no guard is removed.
Logical model/field pins, scalar domains and absent/value decoders are unchanged.
This binding is host-declared physical correspondence, not a compiler proof of
compatible schema evolution. Ashlar must admit both original accepted revisions,
preserve prior fields/pins/keys, and compare the complete present+absent result bag.

Version impact: additive backend-owned binding input; no compile response/IR
shape change. Proposed integration release is backend `0.1.1-compatible-revisions-candidate` with candidate capability `binding.compatibleSchemaRevisions`, retaining existing dialect/IR/binding profile. The isolated source manifest stays historical pending approval; version/capability wiring must precede any distribution. Owning CONTRACT-002 has the additive desired-state paragraph. This isolated patch is not released support.

Two focused compiler tests are authored (legacy guard; selected-set parameter
emission and malformed selections). EXECUTED with root-selected private Rust 1.90.0 and one build job: both PASS, 16 unrelated tests filtered out, test execution 0.02s after 49.85s initial compilation. Locked dependencies fetched into the private Cargo home. Sandbox DNS attempt stopped/reaped130; escalated same command terminal0/reaped 2f979c. No native query or original checkout changes. Independent source review is required before integration.

Patch: compatible-revisions.patch. Modified paths: binding.rs, candidate.rs,
tests/ashlar-databricks/compiler.rs. Candidate.rs was clean in original worktree;
binding.rs and compiler tests are occupied by ongoing dirty work, so merge must
be coordinated by the original owner.
