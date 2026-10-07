# Seventh input and publisher integration preparation — R585–590

The corrected local seventh generator selects source positions600000..699999
from the invertible104729 permutation modulo40M. Its100k identities cannot
intersect prior positions0..599999. It verifies90k updates/10k deletes, all exact
property-token replay, retained/unknown envelope content, large string cursors,
source/event origins and typed endpoint closure. Five corruption controls refuse.
This is a synthetic version1-to2 profile, not producer authority or native apply.

Four normalized input roles contain100k raw records,216667 property events,
10k tombstones and90k complete after-carriers. JSONL totals1100633880 bytes in35
parts, each at most32MiB and rows at most1MiB, within1.2GB local bound. Every part
is independently read back with exact byte/SHA, field ordering/counts, complete
known-field multisets and original input-text multiset hashes matching the
separately generated oracle. Data lives at/private/tmp/ashlar-seventh-input-r586,
outside Git; rehash before transfer because local files are mutable. Code and
compact manifests are committed. No volume or native Delta input has been written.

The independent CDF oracle covers190k complete images each for edge and forward
adjacency; the canonical tombstone oracle includes all ten target fields. The
input-only apply_batch_id tombstone extension stays in original input_json and
is not a canonical DDL addition. Oracle generation23.650920125s, CDF/tombstone
preparation19.204751042s, file generation21.594253625s and readback7.381493542s are
local preparation measurements; some preparation overlapped. Do not sum them
into an uninterrupted publication or source-arrival clock.

R585 ran the generation/refusal loops but stopped at a stale provenance filename
before writing its oracle. Its source and explicit stopped receipt are retained.
Corrected R589 separately regenerated the complete accepted oracle. No native
statement, input or publication came from the stopped run.

`seventh_guard_publish_r596.py` is the concrete next publisher entrypoint, derived
from completed R537 without modifying/replaying it. It targets a new seventh
manifest, inherits only the qualified sixth vector, keeps the atomic full20field
MERGE guard, all15 concurrent content/global checks, five-role closed commit
interval checks, exact descriptor/readback and budget telemetry. The serial
closing-profile loop is replaced by the qualified R553 close_after wrapper at
both prepublication and post-readback checkpoints, with full accepted metadata
retained separately and schema/profile checks reported in the descriptor.

This entrypoint is syntax-checked, not natively executed. Mandatory paths reserve
R593 native input audit, R594 publisher bounds and R595 fresh eligibility; these
receipts do not exist yet, so executing now refuses before client creation. The
seventh ready-input clock can be claimed only after actual staging, fresh UUID/
head/profile qualification and one bounded execution. It cannot be inferred from
component timings or the existence of this code. No new fence, takeover, ACK or
consumer/runtime support is implied. Next transfer only rehashed parts to owned
new volume paths, validate and pin all native inputs, derive limits from the
sixth vector, then execute the integrated publisher once. All original read/
ingest/scale goals remain open;64MiB default and UC Delta remain selected,
external engine limits unchanged and UMF deferred.
