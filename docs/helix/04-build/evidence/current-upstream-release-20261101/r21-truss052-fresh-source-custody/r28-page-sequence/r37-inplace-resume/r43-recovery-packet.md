---
ddx:
  id: ashlar.evidence.r43-r42-no-replay-recovery-20261101
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-002
      kind: informed_by
    - id: CONTRACT-003
      kind: informed_by
    - id: TD-004
      kind: informed_by
---

# R43 retained-publication recovery packet

R42 contains a committed seven-change seed publication in the original six-role Delta destination, with predecessor `truss-graph-cbf69ea847e30930ec586b2a3d508745f0430da3e9e1628f6b81149b9d58230b`. Its immutable pending receipt says `publication_returned_native_readback_pending` and `acknowledgement: not_attempted`. R43 is a no-replay recovery candidate: it restores the existing applied artifact from the original coordination journal, independently verifies the existing Delta vector, durably records native readback, then opens the public Truss .52 source-custody session and ACKs only the selected page if exact reconciliation succeeds. It does not call a publisher, resume a continuation, or replay the seed.

R43 is prepared for review only. No R43 PostgreSQL, Spark, Delta, or Truss effects have run. The current source and destination remain the stopped R36-owned state from R42. The packet has local-development authority only. It binds one local attempt, 900 seconds, 2 GiB process-plus-owned-PostgreSQL RSS, 512 MiB aggregate monitored output/PGDATA/Delta storage, no cloud, and no externally reachable listeners.

The selected consumer value is a typed `FeedConsumer` snapshot built from the newly read public `FeedPage`; it is not presented as a separately fetched consumer result. The pinned .52 API has no non-mutating consumer getter. Before ACK, public `open_feed_source_custody` locks the actual consumer row, checks its current checkpoint equals the page's prior cursor, validates registration/runtime, and rereads the exact selected page. R43 does not re-enroll the consumer.

R43 binds the pending receipt SHA `74752eed6a4529de9ac87eb0c9cbc9ceff82fbf26e4d9fb88194607d1b5b4b88`, publication intent SHA `0ac8a41387eba0275695aff0e52628ff525be5a7e0a3befd023c8bdfb5aec34a`, source-page archive SHA `fe63c1affe12bf60a15fdeb505aee09b925df74d0c40618646aa4b9835970cf6`, public page SHA `0248fd1607b971f267df71b1262668930a96925a767855dd3400fb0ecd1a6456`, stopped PGDATA tree SHA `5f31dc853e4e751d71770f767b17f6800dfda4022ef0cc5efcc9b0642aaffe84`, and SQLite coordination journal SHA `6084db7eddfc116020b34d7a213dd2a8010cefbf0c2c6f6c0c58e60372842aec`. The immutable journal check reports `integrity_check=ok`, counts `{"local_operation": 15, "local_plan": 2, "local_publication_artifact": 2, "local_source_plan": 2}`, and one artifact for the selected R42 request. SQLite WAL and SHM are absent.

The actual R37 supervisor preflight passed with both fresh R43 roots absent while monitoring the existing PGDATA and Delta roots. The R43 worker structural preflight passed against the pinned runtime overlay and actual continuation parser without opening PostgreSQL or Spark. See [effect-free supervisor preflight](r43-effect-free-preflight.json) and [worker structural preflight](r43-structural-preflight.json). Four focused coordinator tests pass, including native-readback refusal before ACK and preservation of an unknown ACK outcome marker.

ACK is gated by exact original descriptor restoration, complete-vector native readback, a durable readback-complete marker, same-session source-page reread, and a durable ACK-attempt marker. An ACK error after that marker is treated as potentially unknown and is reconciled rather than replayed.
