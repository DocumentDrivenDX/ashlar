# R05 canonical GraphFrames one-attempt packet — R4 candidate

R4 prepares one bounded local attempt candidate from Sol-reviewed R3 at main
`c81513113dd31cfedc29507696873e212ca8c17f`. The invocation packet SHA-256 is
`379c451ca3def324dfabb6e06ed9ad02f13b3df8615253911db2f8189e9811d2`; the
proposed authorization document SHA-256 is
`e4cb45ec63e56ee8e7173c4d40b740501f07f2078b0a8d4cb92c86e1092d39b4`. It does not run Spark, start a native process, create the proposed attempt
parent, or issue execution authorization. Sol independently reviewed the
corrected R4 packet under review ID
`sol-independent-review-2026-10-23-r05-r4`; the packet and cleanup checks were
clear. The packet remains blocked pending fresh read-only slot evidence and a
separate owner decision.

The candidate reuses the immutable R3 runtime-tree inventory at
`../r05-native-action-r3/runtime-tree-inventory.json` by its exact absolute path
and SHA-256 (`72244f1dca10d58c96b71bb16aa2e17a29c091e477a725b778e8a30d8bfcf411`).
It does not copy the 6.5 MB inventory. It also pins the reviewed R3 supervisor, worker, and accounting module bytes,
all action and anchor inputs, and the mandatory Python executable, Java
executable, `spark-submit`, and three runtime JARs by exact path and SHA-256 in
`invocation-packet.json`. The packet closes both `retainedInputs` and
`retainedInputSha256`; the focused gate test reconstructs the supervisor’s
mandatory set and checks each file digest. Any byte, metadata, symlink, or path
drift is expected to refuse admission under the reused R3 supervisor/accounting
checks.

The proposed future attempt parent is
`/private/tmp/ashlar-r05-graphframes-r4-c8151311`; that path is currently absent
and no private sidecar or directory was created. The separate
`authorization-proposal.json` has `status: proposed` and binds the invocation
packet digest. It is not an authorization file. The packet remains
`reviewStatus: independently-reviewed` with review ID
`sol-independent-review-2026-10-23-r05-r4`,
`executionStatus: blocked-proposed-authorization`, and
`executionAuthorization: not-issued`; the reused supervisor refuses it before
attempt creation.

The proposed hard bounds are exactly one attempt, 300 seconds total, 2 GiB
retained plus attempt bytes, 512 MiB attempt-root bytes, and 128 MiB diagnostics.
Spark scratch remains inside the fresh accounted attempt root. The runtime is
pinned to `local[2]`, Spark 4.2.0, Delta 4.4.1, GraphFrames 0.12.3, Python
3.12, and JDK 21.0.13. These bounds describe a candidate only.

R3 measured 1,567,274,503 retained bytes including its inventory and invocation
packet, leaving 580,209,145 bytes below 2 GiB. With the full 512 MiB attempt cap
reserved, approximately 43.3 MB (41.3 MiB) remains for authorization and other
additional bounded inputs. R4 currently carries a 13,739-byte invocation packet and a
451-byte proposal; it references the existing inventory and reviewed modules,
so it does not add another 6.5 MB inventory copy. This is an estimate from R3's
recorded baseline, not a fresh live retained-tree accounting pass. Any later
authorization and final packet bytes must fit under the same combined limit.

Before any authorization decision, capture the fresh read-only observations in
`native-slot-evidence-requirements.json`: owner/lease clearance for the exclusive
native slot, current Spark/JVM/Python process inventory and reconciliation,
absence of the proposed attempt path and prior markers, available filesystem
capacity, and the exact host/time. Preserve command output and digests. This
check must not signal processes, perform cleanup, create directories, or start
Spark. It must be repeated immediately before any separate authorization.

Sol review caught that the first R4 draft omitted mandatory retained pins for
Python, Java, `spark-submit`, and the runtime JARs. R4 now includes those pins
and the action/anchor inputs, and the focused inert gate checks pass 4/4. They verify candidate status refusal,
exact packet binding of the reused inventory and reviewed module bytes, closure
of every supervisor-mandatory retained pin (including Python, Java,
`spark-submit`, action and anchor digest, and all JARs), and absence of the
proposed attempt path. They import neither Spark nor PySpark. No native evidence,
publication, Unity Catalog, source progress, protected ACK, production authority,
or scale qualification is claimed. The 1B/5B scale trial remains separately
held.

## Remaining gates

- Fresh read-only native-slot evidence and recorded clearance from the current
  slot owner. The local native slot remains held; the R3 slot note is historical
  context only.
- A separate explicit owner authorization binding the final invocation packet
  digest and an empty, private attempt parent. The current proposal is not it.
- Only after those gates: perform the single local attempt within the exact
  bounds, then retain and review its bounded receipt. No retry is authorized.
