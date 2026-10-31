---
ddx:
  id: ashlar.evidence.truss051-r17-pre-effect-failure-20261031
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r17-native-candidate-20261029
      kind: informed_by
    - id: ashlar.evidence.truss051-r16-native-candidate-20261029
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# Truss 0.0.51 R17 pre-effect failure

R17 consumed its exact one-run approval but failed the frozen selected-PGDATA
admission check before any native effect. The supervisor receipt reports child
runtime 6.203 seconds, return code 2, an empty attempt root, no remaining process
group, `phase: none`, `native_effects: none`, and `effects_may_remain: false`.
The worker stopped with `fresh selected 11-file tree differs from frozen
observation`. It had not reached its first attempt-directory creation marker.
PostgreSQL start, migration, Spark, publication, ACK, seed, and continuation did
not run. The R17 approval is spent; it authorizes no retry.

The retained supervisor receipt is
`/private/tmp/ashlar-truss-r17-full-lifecycle-a2fe02b9-e33a-4b73-9a13-96fbdd9100ab-supervisor/supervisor-receipt.json`
(1,458 bytes, SHA-256
`1d63eb43cb3273386ba291fe644ffba8add795a11a053efdfe59878da0d34a6b`). Its
stderr log is 458 bytes, SHA-256
`0dc33e8fab7990070053d9e61095f20c6bd511e3d6e45bfb0fcf59eb1a2c786d`; stdout
is empty. The attempt root is
`/private/tmp/ashlar-truss-r17-full-lifecycle-a2fe02b9-e33a-4b73-9a13-96fbdd9100ab`.
The approved scope SHA-256 was
`923f19c42617ed3d7053ba09d1125d698f07110f085c34eb78e2fd6d7ca2b798`.

Read-only inspection found the original selected PGDATA path and original cold
backup path still present as empty directory shells: both currently inventory
as zero files, zero bytes, tree SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`. All 11
selected files are absent. The cause and time of that prior data disappearance
are unknown; R17 itself is ruled out as the cause because it stopped before
output creation.

There is a byte-verified recovery source in the retained R16 attempt. Its
`selected-discrepancy` directory independently inventories to the frozen
selected-state tree SHA-256
`b979795a32e5b6c9f0d9bbda7849f130e20aa2846af96220cbbcbdfce6937c75` (11 files,
17,197,691 bytes), exactly matching R16's `discrepancy-preserved.json` receipt
(SHA-256 `cb0e72c4d7fe631a340d543d46e1173b7fe9cbcdce3eacd9f9dce9895e85fec7`).
The retained `restored-pgdata` independently inventories to
`67f15dff3198ba6b28bbe93a35cb7b595fe0439f84bcb4c14b9ef6d1f4c9effc` (1,248
files, 63,997,052 bytes), exactly matching `cold-backup-restored.json` receipt
(SHA-256 `18264888e9b0feec6cc64a1a4f373f0dcf5fd03a2ea5bbdd53b20ea753fd412`).
These are valid observed input candidates for a newly reviewed packet; this
receipt does not authorize copying, restoring, starting PostgreSQL, or reusing
R17 approval. A new packet must bind both paths and receipt bytes, re-inventory
them immediately before any effect, and retain exact ownership and stop
controls. No R18 approval or execution is recorded here.

R16 itself stopped during its restore phase with `effects_may_remain: true`,
although PostgreSQL had not started; its failure receipt remains relevant
history. R16's preserved copies are kept separate from the missing original
paths and were not modified by this read-only inspection.
