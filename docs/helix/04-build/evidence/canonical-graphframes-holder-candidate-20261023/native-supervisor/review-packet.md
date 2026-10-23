# Bounded native supervisor review packet

Sol independently reviewed the prior supervisor wrapper and worker as inert
preparation. Sol also reviewed the 2 GiB/attempt-root/Java- and Spark-environment
hardening delta in Ashlar commit `8705cd00`; the focused supervisor suite passed
19/19. Sol reviewed the subsequent exact PySpark interpreter binding and its
21/21 focused checks. The checked-in invocation packet remains pending review as a future
executable selection because its runtime and authorization fields are empty.
Nothing here authorizes execution. The wrapper, worker, invocation
packet, action anchor, and action module are independently hash-pinned; the
reviewed action anchor does not transitively bind the wrapper.

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

The wrapper limit is one local `[2]` Spark child, one process group, a total
300-second monotonic deadline including admission, process execution, receipt
validation, retained-input close rehash, and final receipt write; at most 2 GiB
of selected retained-input files plus attempt-root files; an independent
512 MiB attempt-root ceiling; and at most 128 MiB diagnostics (a sub-limit within
the attempt-root ceiling). Attempt, fixture, Spark scratch, temporary
directories, result, diagnostics and receipt files stay under a fresh
mode-0700 root. The wrapper periodically stats the complete attempt tree and
kills/reaps the process group at a detected breach. It does not install a
filesystem quota, so these are periodic monitored limits, not instantaneous
kernel-enforced ceilings. The 2 GiB monitored sum applies to enumerated
retained-input files plus the attempt-root tree. It does not itself enforce full
Python, JDK, or PySpark distribution-directory accounting. Those distribution
trees must be separately measured and included in an all-in <=2 GiB proposal
before authorization; the future packet must state how their size is monitored
and the limits of that monitoring. The current inert packet does not claim that
complete envelope.

The child environment must bind an exact canonical `JAVA_HOME`, its
`bin/java` executable path and SHA-256 as retained inputs, and an exact canonical
`SPARK_HOME`, its exact `bin/spark-submit` executable path and SHA-256 as a
retained input, and a `jars` directory.
The wrapper sets both environment variables and a controlled `PATH` beginning
with the pinned Java `bin` directory; ambient Java selection is not used. Both
`PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` point to the same exact retained
Python executable whose path, executable bit, and SHA-256 are checked before
launch. The checked-in packet leaves all runtime path/hash fields and JAR paths
null and remains execution-disabled.
Any populated runtime requires a new immutable packet and independent review.

The worker is expected to verify the reviewed action anchor and exact runtime
pins, persist the finite synthetic fixture, execute the single canonical
action, and return a bounded receipt. No native execution has occurred. The
only intended claim is a future local, pinned, synthetic one-hop execution
candidate. This provides no source progress, protected ACK, publication,
Unity Catalog, production authority, or scale qualification.

## Review inventory

`artifacts.sha256` records the exact wrapper, worker, packet, and focused test
bytes. It is a convenience inventory, not a signature or authorization.
