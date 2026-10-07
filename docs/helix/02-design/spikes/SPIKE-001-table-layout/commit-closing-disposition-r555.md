# Post-write closing binding — R553–555

This private spike iteration extracts the missing publisher-to-closing binding
from the completed sixth publication. It does not execute another publication,
change table data, or claim a new processing clock.

`publisher_commit_closing_r553.expected_after` preserves initial ten-table UUID,
schema and protocol expectations, replaces exactly five mutable base heads with
independently recorded commit events, and requires each event's statement ID and
single-version interval to match the publisher vector. The selected node pin
remains unchanged; its physical head is deliberately retained from initial
custody. Inputs retain their original heads. `close_after` delegates this complete
expectation to the already native-qualified concurrent closing module R548.

R554 binds actual audited R537 events to all ten native R549 closing observations,
checking full schema, UUID/format, protocol/property/clustering profile, version
and underlying statement ID. Eight adversarial cases refuse: missing event,
duplicate role, foreign statement binding, unexpected version interval, changed
UUID, changed selected node version, mismatched initial mutable head and missing
input custody. Initial metadata remains unchanged. This is offline integration
binding evidence; it does not substitute for native execution of the wrapper.

For the next authorized publisher, retain the R537 closed commit-interval checks
before publication and after descriptor readback. At both points call
`close_after(workers, initial_expected, base, current_tables, commit_events, phase)`
with separate durable phases, and retain accepted results in the publication
receipt. Close consumed worker cursors and collect final telemetry at the existing
budget checkpoints. Keep atomic full-carrier predecessor guards, independent
content validation, exact manifest vector/readback and all resource bounds.
Never replay the completed R537 entrypoint to exercise this change.

The previous component clocks (3.38-second concurrent versus 11.14/11.44-second
serial comparison; extracted module 4.90 seconds) are observations on existing
clients and compute, not an inferred whole-publisher improvement. Full sixth
processing remains 221.83 seconds; the current-table MERGE alone remains 64.68
seconds. Warm singleton caller/engine p95 remains 407/102 ms in the scoped E9
sample. Provisional performance, sustained arrivals, burst and 1B/5B admission
remain open. Metadata observations and closed intervals do not constitute a
production writer fence. UC Delta remains selected; consumer mapping limits and
UMF deferral are unchanged.
