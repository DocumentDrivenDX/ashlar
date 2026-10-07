# Native absent-ID races and recovery r365–r368

Governed by CONTRACT-001–003 and proposed ADR-001. Private three-column manifest references only, on the existing authorized warehouse; no production pointers or source ACKs.

Two fresh private shallow clones of manifest r360 at version3 independently begin with the two exact inherited descriptors and no r365-race ID. Two simultaneous persistent clients per clone run the conditional descriptor MERGE. One pair uses identical text/digests; the other uses conflicting owner content. Both pairs produce exactly one new race row and one successful writer; the other writer fails with DELTA_CONCURRENT_APPEND.ROW_LEVEL_CHANGES. Native statement windows overlap2469ms and2282ms respectively; these include planning/queue and do not prove identical read snapshots. Both complete commit ledgers are clone0/MERGE1 under recorded statement IDs, with native UUIDs and exact unchanged baseline descriptors verified.

Race phase24.343s reports56,600read bytes/7,482write bytes/0spill across18final native statements, including two failed attempts. No client retry is hidden inside the worker.

A separately admitted recovery verifies each original handle has terminal FAILED/final native history, then rechecks the owned UUID, unchanged head and exact winner before executing one explicit attempt with the original SQL/parameters. Identical replay succeeds; conflicting recovery fails inside MERGE with ASHLAR_PUBLICATION_ID_CONFLICT. Complete readbacks remain exact and conflicting head stays1. Recovery11.810s reports50,413read bytes/0write/0spill across12successful and1failed native-final statements. Both phases combined report107,013read bytes/7,482write bytes/0spill; phase clocks are separate and their sum is not an end-to-end recovery SLA.

## Design consequence

Handle a concurrency conflict as an observed transaction outcome: capture its native ID, resolve terminal status, inspect the winner and admit a bounded recovery only after checking identity/custody. Exact duplicate remains acceptable; changed descriptor content remains refused. An observation timeout or unknown submission outcome still requires handle/history recovery rather than resubmission. Retry exhaustion preserves the previous complete publication and returns a conflict/recovery outcome; do not loop indefinitely on compute.

These two witnessed races support this specific conflict-and-recovery path under the recorded runtime/profile. They do not enforce uniqueness for every future writer/schema/isolation configuration, fence current/raw/journal/projection writers, or establish one global graph transaction. Keep a single logical publication authority as the proposed profile and qualify its ownership mechanism separately. No new absent-ID uniqueness support claim or generic production fence follows.

The two-role descriptors remain reference records, not complete current/raw/journal/tombstone publication vectors. Next integrate this bounded conflict path with a full-role private publication reference and explicit authority token, retaining every input/history/carrier validation before advancement. Native singleton, freshness, sustained/burst and1B/5B gates remain open and unchanged. UMF bindings and external graph-engine qualification remain separately scoped.

Evidence: [race audit](out/native/ashlar_manifest_creation_race_r365/audited-summary.json), [recovery audit](out/native/ashlar_manifest_race_recovery_r367/audited-summary.json), reproducible workers and independent offline audits. Reported failed-write bytes do not establish retained/orphan storage; no VACUUM or cleanup occurs.
