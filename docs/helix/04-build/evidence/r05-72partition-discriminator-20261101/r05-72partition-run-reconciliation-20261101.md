---
ddx:
  id: ashlar.evidence.r05-72partition-run-reconciliation-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-003
      kind: governed_by
    - id: TD-008
      kind: informed_by
    - id: ashlar.evidence.r05-72partition-discriminator-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-release-scale-review-20261101
      kind: informed_by
    - id: ashlar.evidence.r05-one-shot-full-scale-run-packet-20261029
      kind: informed_by
---

# R05 72-partition attempt reconciliation — 2026-11-01

## Outcome

The authorized local candidate completed its worker and semantic checks in
53.324 seconds. It wrote the requested 45,000,000 node rows and 225,000,000
edge rows in one sequential attempt. The worker reported 72 source partitions,
72 output files per table, exact row/value summaries, and zero invalid edge
endpoints. Node and edge write times were 6.623148 and 21.681731 seconds; the
combined write wall was 28.306671 seconds, followed by a 14.565078-second
persisted witness.

The supervisor retained a `failed-or-held` result with reason
`write-stage-task-attribution-not-exactly-72`. This is a validator false
negative for the tagged write phases: event stages 12 and 13 independently
identify `node_write` and `edge_write`, and each has exactly 72 tasks. The
validator also required every event-log job and task stage to have a phase tag.
Sixteen metadata/Delta jobs did not carry this attempt's job-group tag, leaving
14 task stages unattributed globally. That global gap is diagnostic and does
not contradict the two directly tagged write stages. The code correction in
the packet now gates on parsed events and the exact phase-tagged write stages,
while retaining global attribution status for review.

The run itself remains **held**, not retroactively passed: the supervisor did
not finish its post-run table cleanup because the false-negative guard fired.
At the time of initial reconciliation, the receipt-owned root remained
untouched for Sol review at
`/private/tmp/ashlar-r05-72partition-attempts-0a7a473c/ashlar-r05-72partition-fdde904eff007b3039fe615e`.
No additional Spark run or cleanup was performed for this reconciliation.

On 2026-11-01, after the retained receipts and table-size witnesses were
reviewed, the generated `candidate-72` Delta table directory was removed to
recover 2.9 GiB when the local disk fell below 120 MiB free. The supervisor,
worker, event-summary and ownership receipts, Spark event log, and this
reconciliation remain retained. This cleanup does not retroactively pass the
held run; independent rereads of its raw Delta tables are no longer possible.

## Reconciled evidence

The supervisor receipt records packet SHA-256
`4233e056f437c4c9d7a5ffe691549cd59a4e335dbe39ee0b88ca69f4d673c82b`, source
pins `c67f146f1b03fcfb19e532fd715ab3ac4ae0e619eddba33412e0ef1ebdab6996`, and
runtime pins `ad5a84b7184fa9c02964932362d42cc14ffe6340cedddd7020f26a63d5253d44`.
It records child exit 0, process-group quiescence, 53.303792 seconds before
cleanup, and the held outcome described above.

The worker observed Spark/PySpark 4.2.0, Delta 4.4.1, JDK 21.0.13.0.2 and
`local[18]`. Its range witness reports 72 slices for both roles. Both tables
are at Delta version 1, writer protocol 7, and use the pinned schema and
protocol receipts:

| Table | Rows | Files | Table bytes | Schema SHA-256 | UUID |
| --- | ---: | ---: | ---: | --- | --- |
| `object_current` | 45,000,000 | 72 | 474,440,236 | `e6689973a5cf0abda88618bc08a4deef7b4904ac51c1ec5a83e90cf6a38dfa8a` | `6ab11ffa-6a5a-4c72-b8e8-de34df503ec5` |
| `edge_current` | 225,000,000 | 72 | 2,641,915,936 | `bf6657de5fd7839e327c10e94b155dc2faa9e302c741cc351fd1bfa97ae59ce1` | `3e9567d9-7929-4266-8053-af52e2cbf7c2` |

The row/value witnesses report expected counts, minimum ID 0, the expected
maximum IDs, exact arithmetic sums, and zero bad rows. The independent edge
formula witness reports 225,000,000 rows with zero invalid source/target types
and zero out-of-range endpoints. The candidate's aggregate witness explicitly
does not prove exact distinct-key uniqueness.

The Spark event log has 1,295 records and SHA-256
`351ad098d25019df05f3c4ea856ab49491f38e20961f9ce5757d5523496f1f65`. The
phase-attributed write task summaries are:

| Phase | Stage | Tasks | Task duration min / max | Executor CPU | GC | Memory / disk spill |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Node write | 12 | 72 | 866 / 3,014 ms | 88.154 s | 1.670 s | 0 / 0 B |
| Edge write | 13 | 72 | 4,807 / 5,646 ms | 326.413 s | 6.882 s | 0 / 0 B |

The corresponding summed task durations were 111.310 and 371.516 seconds. They
are aggregate task time, not wall time; the measured write wall was
28.306671 seconds on `local[18]`. The event summary remains marked globally
`incomplete` with 16 untagged jobs and 14 unattributed task stages, while the
two write stages above are individually tagged and complete.

The preserved supervisor, worker, event-summary, and final-ownership receipt
SHA-256 values are respectively:

- `r05-72-supervisor-result.json`: `eb2f6921e3b90065e56cf28fea76f3778163b5f06bbdb4030f06d91bb9a1946e`
- `r05-72-worker-result.json`: `40b815c45990992175ef323f046df8e09079d1664dceefa7c09903dc7ee14d20`
- `r05-72-event-summary.json`: `52f04a339b8c4d79c48fd2efc194745e1b882bfa09b5444ff4c67091302a8662`
- `r05-72-ownership-final.json`: `cabfa9557edc479d8e6b90b28a642d2c42820d5233b90b613739d82d4bd22731`

## Retained-baseline comparison and scale arithmetic

The matching retained sequential 18-partition sample wrote the same 45M/225M
workload in 35.227 seconds and reported 3,041,334,560 combined table bytes.
The new 72-partition candidate wrote in 28.306671 seconds, a 19.64% wall-time
reduction (24.45% higher measured throughput) in this one local sample. Its
combined table size is 3,116,356,172 bytes, 2.47% above the retained sample.
This compares one candidate against the existing evidence; the baseline was
not rerun.

Using the same linear workload factor `1B / 45M = 22.2222` and an 80%-of-sample
rate assumption, writer-only arithmetic is 13.105 minutes for this candidate,
versus 16.309 minutes for the retained sequential baseline. For the candidate's
measured setup/write/compact-witness bundle, `48.464554 × 22.2222 / 0.8` is
22.437 minutes. The analogous retained sequential bundle was 30.684 minutes.
These projections assume linear scaling from one small local run and do not
predict target-scale completion.

The writer-only projection is below the 15-minute review trigger but above the
provisional five-minute working target. The measured bundle projection is
above the 15-minute trigger before the separate point reads, second persisted
scan, GraphFrames action, and final cleanup. Therefore this measurement does
not clear the full-path review gate, establish production-scale performance,
or change the existing hold on a 1B/5B attempt. The output-file increase and
unknown scale effects also need review before carrying the 72-slice choice to
any larger compute shape.

## Future validator correction

`r05_72_partition_supervisor.py` now has a phase-local validator that requires
both write phases to be present in a parsed event summary, unambiguously tagged
to their own phase, and to contain exactly 72 tasks per tagged stage. It no
longer treats untagged Delta metadata jobs as grounds to reject independently
attributed writes. The global `stagePhaseAttribution` field remains in the
event summary as a diagnostic. A focused test reproduces the observed shape:
two exact 72-task write stages plus unrelated untagged stages passes; a
71-task write stage or missing phase fails. This correction is for future
reviewed attempts only and does not rewrite this attempt's supervisor receipt.
