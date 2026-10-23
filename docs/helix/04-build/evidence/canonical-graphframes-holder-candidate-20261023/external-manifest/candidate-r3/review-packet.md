# Canonical action manifest r3 — independent review record

Status: **independently reviewed for the closed synthetic action descriptor**.
Review ID: `sol-independent-review-2026-10-23-canonical-action-r2`.
This decision binds the unchanged action module SHA-256
`be40300c0cd48d0075d14cf98555b154f1d0b4940991bc6fdf587536646757aa`, the
three-row/two-edge synthetic source, the independently authored two-occurrence
oracle, the generated `.4` carrier definition, and the exact manifest and
anchor bytes in this directory.

The manifest retains the exact selected seed, cap 2, cap-plus-one collection
bound, output field order, row counts, formulas and finite formula proofs from
r2. It claims only that the descriptor inputs were independently reviewed.
Persisted carrier counts and values still require the bounded native witness;
the action entrypoint requires that witness before constructing GraphFrame.
No native execution attestation exists in this packet. It grants no Spark run,
source progress, protected ACK, publication, Unity Catalog access, production
authority, or scale qualification.

The separately developed `native-supervisor/` wrapper and worker are not bound
by this action anchor. Sol independently reviewed the exact wrapper and worker
as inert preparation; that review does not authorize execution. The invocation
packet binds their bytes separately and remains pending and execution-disabled:
the local interpreter and JAR paths are unset, and a fresh exclusive Spark
slot plus separate user authorization are required. The packet must be
immutably re-frozen with reviewed status and exact runtime/accounting inputs
before any run can be admitted. No native execution has occurred.
