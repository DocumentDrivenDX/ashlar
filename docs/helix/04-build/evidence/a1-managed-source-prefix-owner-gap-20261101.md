---
ddx:
  id: ashlar.evidence.a1-managed-source-prefix-owner-gap-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: CONTRACT-004
      kind: informed_by
    - id: TD-009
      kind: informed_by
---

# A1 managed data-prefix owner dependency gap

Read-only source review on 2026-11-01 found that Ashlar has the `.4` resolver
path but no admitted owner that can currently supply its independent source
prefix for a live A1 interval. No Spark, UC, Truss, cloud or ACK effect was run.

`ManagedCurrentSparkReceiptReader.reopen()` already accepts exact
`ashlar-managed-current-source-prefix/0.1` bytes, their digest and the matching
`ManagedCurrentCoreConfig`. For `.4`, it derives the typed lookup keys from
those bytes, probes native Spark `xxhash64`, rebuilds the source oracle and
compares all four exact pinned-version row bags. This is the required resolver
readback seam; the current gap is admission and delivery of its original inputs,
not a missing native hash or row comparison operation. The
`ManagedCurrentTrialConfig.core_config()` provides the typed `.4` config for a
selected trial namespace. `examples/managed-current-trial/notebook.py` passes a
private source oracle to its publication/query campaign, but it neither
constructs nor returns the original complete source-prefix owner package needed
by the external A1 holder.

The existing `/0.1` prefix is insufficient for the governed A1 state. CONTRACT-003
and TD-009 require A1 to preserve both A and B progress entries while A1
advances only A. The proposed `/0.2` package validator
(`validate_selected_managed_current_prefix_v2`) checks correspondence against
separately selected anchors; it does not select or authenticate them. A focused
13-test update now checks typed A/B genesis cursor identities, complete
page-absent initial markers, and each retained request against its native page
bytes and cursor transition. This remains a pure correspondence check. Its
required anchors are original A/B registration objects and exact genesis cursor
bytes with typed feed/consumer/ACK identities, the complete original two-feed
Ashlar progress JSON, and the original publication manifest ID and bytes. It
then requires the exact R04 schedule, destination and retained page/request,
materialization clock and publication manifest chain.

The required complete initial progress and genesis publication have no current
source-owner evidence/API in the selected Ashlar flow. A Truss genesis cursor
does not contain the feed, page payload digest or batch identity needed to
construct an Ashlar `truss_checkpoint`; deriving progress from the cursor would
invent state. Truss `.52` can expose its own selected registration, consumer,
cursor and page observations, but those values do not establish Ashlar's
two-feed initial progress or publication manifest. The managed trial worker
starts a fresh schema and returns the task receipt after its page publication;
it does not return the required independently retained initial A/B custody
package. The existing Truss `.4` source-metadata envelope is not a substitute
for the managed graph-data prefix.

The next implementable seam is an owner-produced, exact input record for the
`/0.2` correspondence validator. Its source owner must independently obtain
and retain the two original Truss registrations/cursors and identities; its
Ashlar publication owner must independently retain the initial complete A/B
progress and manifest; the scheduled publisher must retain exact per-page
bundle, request, materialization clock and committed manifest bytes. The owner
must select these anchors before comparing the package and must refuse if any
are missing. Once that admitted package exists, the current `.4` reader can be
called with its exact bytes/digest and matching native Core config at open,
pre-ACK and close. This finding makes no claim that the adapter or its fake
tests qualify managed UC, native Core behavior, Truss custody, or ACK authority.
