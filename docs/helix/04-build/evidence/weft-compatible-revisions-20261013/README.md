# Compatible-revision query integration

Ashlar's pure record selector derives compatible retained revisions from accepted
Truss record allocations, preserving every prior field pin, key and required
field. Its two basic controls pass. The selector grants no source or query authority.

The isolated Weft patch adds a bounded explicit revision set and parameterized
binary-collation integrity guards. Both focused compiler tests pass on pinned
Rust 1.90.0; independent review found no blocker. The original Weft working tree
was untouched. This is an unreleased integration proposal, with no native query
pass. Binding integration must refuse sets above the compiler limit of 32 rather
than trim the selector's complete result. Candidate version/capability wiring and
upstream integration remain required.
