# First new-edge extent oracle and native prefix: R656–R664

The independent local first8M-edge oracle completed in717.713 seconds. It covers
8M carriers,8M raw records,32M property events and8M forward adjacency records in
80 bounded100k-entity chunks. R659 verifies complete receipt coverage and source
provenance; SHA256 multiset comparison retains its collision assumption.

The existing2.5M-edge native slice now matches the first25 local chunks across
all four roles and every field. R663 audits22 exact final native statements and
100 complete-field digest groups, including native schema types and absence of
an invalid-membership bucket. The read-only phase took101.663 seconds and read
6,083,495,060 bytes with zero writes/spill. Closing observed heads remained stable.

R660 stopped after13 terminal metadata statements because adjacency had an
OPTIMIZE commit at version1; the assumed version0 head was stale. Its exact final
stop receipt is preserved with zero recorded data costs. R662/R663 explicitly
verify the observed heads: carriers0/raw0/property journal0/adjacency1. They do
not silently substitute the original pin or claim an atomic writer fence.

The pinned16M typed node union and full5M endpoint-reference closure remain
qualified by R654/R655. This new edge proof is across separate private physical
extents, not a single16M-node table, full80M-edge graph or new publication.
Existing published vector N6/E10/R7/J7/A8/T7 and edge maintenance head12 are
untouched. No ACK, canonical write, cleanup or compute-setting change occurred.

R657 conditionally bounds growth through the first8M new edges at75GB read,
35GB write and2,700 native seconds, with500k-edge chunks and a stop on spill.
The next controller must revalidate the observed owned heads, preserve the
adjacency version1 maintenance advance, attribute all appends, compare the full
first8M extent to this local oracle and recheck endpoint closure. This budget
is based on measured all-role costs with headroom, not billing or billion-scale
admission. Full40M new-edge growth and singleton/publication gates remain open.
