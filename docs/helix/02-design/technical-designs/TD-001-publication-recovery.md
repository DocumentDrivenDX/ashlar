---
ddx:
  id: TD-001
  type: technical-design
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: ADR-001
    kind: informed_by
  - id: CONTRACT-001
    kind: informed_by
  - id: CONTRACT-004
    kind: informed_by
  - id: US-004
    kind: informed_by
---

# Publication and recovery assurance design

## Scope and technical approach

US-004 replay trust applies CONTRACT-001 publication atomicity and CONTRACT-004
original-operation/read custody. ADR-001 owns the component map; this slice adds
no shared interface or storage layout. Publisher/recovery retain original intent,
manifest/store ports retain complete native vectors, and the resolver/protected
ACK boundary rechecks authority before exposing results or source progress.

## Component and test correspondence

`publisher.py`, `stored_publisher.py` and `recovery.py` own attempt transitions;
`publication.py` owns descriptor/read closing admission. The protected-source
ACK adapter owns source-bound progress and unknown-commit reconciliation.
Native transport and pin ports remain injected. Apply ADR-001 dependencies and
`python3 tools/check_module_boundaries.py`; semantic review covers dynamically
loaded host policies and source profiles. TP-001 owns tests for US-004 acceptance
criteria, including success, exact replay, changed intent refusal, publication
before ACK recovery and closing-context refusal. No interface or source semantic
validator is introduced by the assurance model.

### Paths held-read correspondence

CONTRACT-004's selected Paths and Paths-keys reads preserve the existing state
model and precise-specification assurance level. `ashlar_host.path_admission`
owns original compiler/model/binding and obligation admission;
`path_execution` owns the guarded held interval, complete schema observations,
checks, bounded capture, descriptor-selected decoding and final same-vector
recheck. Its returned evidence is provisional. `paths_query` owns final source,
compiler, protected-ACK and publication correspondence, reader/client closure,
Spark shutdown and complete report publication.

PUB-F1 maps to opening/closing original publication equality, interval closure
and the outer release gate: any admission, native, decoder, closing or cleanup
failure withholds success. Test drift, partial capture, cancellation and
reader/Spark close failures through these public boundaries. PUB-F2 maps to the
read-only workflow and exact opening/closing protected-ACK readback; execution
has no progress-mutation port. A successful query is not a new publication or
source ACK. Preserve the primary failure through cleanup; a report published
after all gates does not promise power-loss durability.

One-hop admission and decoding extend the checked representation and guard
inventory, not publication or ACK transitions. Review this unchanged-model
disposition and its mapped controls after profile/host changes. Key order,
bag enumeration, native capacity and arithmetic remain outside this publication
model and require independent semantic/native evidence. No new analyzer run or
mechanical assurance follows from this correspondence.

## Formal Specification: publication and ACK

Scope FR-4/US-004 and CONTRACT-001/004: reviewed precise specification of
publication visibility, original attempt custody and independent feed ACK
progress. Initial assurance is precise specification plus implementation tests;
bounded executable analysis is the next justified assurance increment for this
crash/replay slice, not a claim already established. Model
state contains source installation/feed/epoch, original request/operation bytes,
prepared/submitted/committed/uncertain phases, complete UUID/version vectors,
immutable manifest ancestry, per-feed progress and exact ACK receipts. In the finite analysis initial state,
feeds start at zero, no publication or receipt is accepted, and installation
identity is fixed. Submission requires retained original intent under the writer
lane; publication requires complete validated effects/vector; ACK requires that
original readable publication plus source/request correspondence. Failure may
leave a submitted or committed-but-unobserved original; reconciliation examines
only that original and refuses replacement while outcome is uncertain.

| Property | Authority | Required assurance |
| --- | --- | --- |
| PUB-F1: readers expose one complete immutable vector, never a mixed refresh | CONTRACT-001 atomic visibility; CONTRACT-004 closing guards | Precise semantic review plus native closing-failure tests |
| PUB-F2: ACK never outruns complete readable publication and exact source batch | CONTRACT-001 progress; protected ACK source correspondence | Precise semantic review plus ordinary-role/native readback |
| PUB-F3: replay retains exact intent and never resubmits an uncertain original as a replacement | CONTRACT-004 original custody/recovery | Precise semantic review plus lost-journal/unknown-commit controls |
| PUB-F4: A→B→A interleaving preserves independent feed/epoch progress and global ancestry | CONTRACT-001 source ownership/progress | Precise semantic review plus admitted two-source oracle |
| PUB-L1: a committed original can reconcile and ACK after availability returns | US-004 recovery | Reachable recovery witness, conditional on fair retries, readable snapshots and available source authority |

A future bounded analysis must select and justify an established model checker
or a reviewed finite transition explorer before claiming mechanical assurance, with
explicit two-feed/two-position domains, failure sites and search depth. Retain
reachable success, manifest-before-ACK recovery and uncertain-commit traces;
deliberately disable each relevant guard and require its intended counterexample.
Map transitions to publisher/recovery/resolver and protected-ACK code/tests.
Model finite identities as exact tokens, preserving separation of feeds and
operations; exclude cloud IAM, uncoordinated administrator mutation, real timing,
physical retention and engine semantics. These require independent host/native
qualification. Incomplete exploration or timeout is unknown, not passing evidence.
