# Bounded native supervisor review packet

Sol independently reviewed the exact supervisor wrapper and worker as inert
preparation. This packet records that review scope; it does not authorize
execution. The wrapper, worker, invocation packet, action anchor, and action
module are independently hash-pinned; the reviewed action anchor does not
transitively bind the wrapper.

The checked-in invocation packet still literally records
`pending-independent-wrapper-review` and
`pending-separate-wrapper-review-slot-check-and-user-authorization`; it remains
execution-disabled and cannot pass admission. Its runtime interpreter and
exact runtime JAR paths are null. Before any possible one-attempt run, prepare
a new immutable invocation packet with the reviewed wrapper/worker hashes,
exact Python and JAR paths and hashes, full accounted retained-input inventory,
fresh slot attestation, review record, and
`executionStatus: authorized-for-one-attempt`. The packet hash must be selected
separately, along with a separately selected authorization record whose digest
binds that exact packet. A separate explicit user authorization and fresh
exclusive-slot check are required. No in-place status edit is permitted. Any
change requires a new packet and review.

The separately selected authorization record must also bind the canonical
mode-0700 attempt-parent path. The one-use marker inside that parent binds the
invocation packet digest, authorization digest, and parent path; selecting a
different fresh parent is refused.

The intended limit is one local `[2]` Spark child, one process group, a total
300-second monotonic deadline including admission, process execution, receipt
validation, retained-input close rehash, and final receipt write; at most 1 GiB
of retained input plus attempt-root files; and at most 128 MiB diagnostics.
Attempt, fixture, Spark scratch, temporary directories, result and receipt files
are under a fresh mode-0700 root. The wrapper periodically stats the complete
attempt tree and kills/reaps the process group at a detected breach. It does
not install a filesystem quota, so the 1 GiB control is periodic monitored
accounting, not an instantaneous kernel-enforced ceiling. Python/Spark runtime
distribution bytes must be included in a reviewed accounting plan before any
authorization; this packet currently leaves interpreter/JAR paths empty and
execution inert.

The worker is expected to verify the reviewed action anchor and exact runtime
pins, persist the finite synthetic fixture, execute the single canonical
action, and return a bounded receipt. No native execution has occurred. The
only intended claim is a future local, pinned, synthetic one-hop execution
candidate. This provides no source progress, protected ACK, publication,
Unity Catalog, production authority, or scale qualification.

## Review inventory

`artifacts.sha256` records the exact wrapper, worker, packet, and focused test
bytes. It is a convenience inventory, not a signature or authorization.
