# Original commerce Truss22 lifecycle and local Delta

Actual original UMF0.8 commerce, installed Ashlar source
`a2b76712f74cbd67804e214623191ac6e4531f95`, wheel
`3bab4a929c723756bf8c372a06c570b1e919d572826ea15488f05365fcf499bf`.
This retains one small local source and its existing publication. Reused selected
Truss22, Spark/Delta and compiler resource configuration is archived; no cloud or
production authority claim is made.

Original STRING projection and RELATED_KEYS queries passed on the predecessor
publication in 21.570 seconds. Public source mutations then passed in 1.978 seconds:
product 3 price became exact Decimal `13.75`; one atomic group replaced required
fulfillment 6 → order line 5 edge 16 with allocated edge 22; another atomically
deleted edge 22 and fulfillment 6. Each command retained complete committed
readback and original semantic replay. Mutation alone did not advance the feed
checkpoint.

Three actual continuation publications completed with complete four-table native
effect parity. **The outer continuation runner failed**, exit 1 after 79.289
seconds, when its subsequent summary accessed nonexistent `FeedPage.changes`.
The failed original log and supervisor result are retained; this wrapper is not
reported as successful. Original source-plan TEXT bytes for all three requests
were captured from SQLite using `mode=ro&immutable=1`. Their exact SHA256 digests
match each manifest's `original_source_plan_sha256`; generated and selected steps
are retained without reconstructed or repaired plans.

Separate read-only postverification passed in 18.301 seconds (19.426 supervised):
all three archived cursor links and fresh source checkpoint matched the final
next cursor; the updated price query returned exact `13.75`; the fulfillment
(type 2, ID 6) singleton was absent; query checkpoint bytes stayed unchanged.
No continuation or source effects were retried. Earlier read-only harness setup
failures supply no positive evidence.

`result.json` separates query, source mutation, continuation and postverification
outcomes. `original-captures.tar.gz` retains 53 original captures, full query
request/response bytes, source-plan bytes, installed wheel, selected runtime
references and terminal logs. `capture-inventory.json`, `installed-custody.json`
and `SHA256SUMS` bind those bytes. Selected installed file custody is not a full
OS dependency closure or a reusable source/ACK authority grant. Additive schema
evolution and submitted-operation uncertainty recovery are outside this result.
