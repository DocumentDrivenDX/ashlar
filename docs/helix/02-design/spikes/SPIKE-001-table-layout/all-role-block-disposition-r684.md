# All-role bounded verifier: R681–R684

All three remaining branches now pass complete native8M-entity parity against
independent local field oracles. R683 audits14 exact final statements and240
digest groups. Whole-table coverage and disjoint block row sums cover all rows.
The phase took111.669 seconds and read13,418,825,258 bytes with zero writes/spill.

Direct carrier ID bounds read51/53 files and pruned53/51. Raw delivery bounds
read56/64 and pruned64/56. Adjacency read4/8 and pruned8/4. These are actual
query metrics, not inferred file-domain pruning. Combined with qualified journal
blocks R677, all four role branches have bounded4M-entity verification evidence.
Combined recorded reads are19,425,462,936 bytes for the full existing8M extent.

R679 updates the conditional16M-new-edge read sensitivity to68,276,388,808 bytes:
double verified full-role reads,1.5x headroom and10GB reserve. Native bounds remain
85GB read/35GB write/zero spill and2,700 seconds; no interrupted wall-rate estimate
is used. Prior canceled-setup costs remain unknown rather than silently zero.

The independent next8M local oracle R680 completed in814.609 seconds. R685
validates all80 next-range chunks and source provenance; R686 combines prior
and next ranges into160 contiguous chunks covering16M new edges,16M raw records,
64M property events and16M adjacency records. Composition is receipt/coverage
validation, not a third independent regeneration or native16M growth admission.
No next-stage native writes have occurred. Fresh UUID/head/schema preflight and
explicit maintenance reconciliation are still required. Published pins, Truss
semantics, UMF deferral and provisional performance gates remain unchanged.
