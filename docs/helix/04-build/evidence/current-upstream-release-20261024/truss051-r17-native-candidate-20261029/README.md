---
ddx:
  id: ashlar.evidence.truss051-r17-native-candidate-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.truss051-r16-native-candidate-20261029
      kind: informed_by
    - id: ashlar.evidence.truss051-r16-effectfree-candidate-20261029
      kind: informed_by
    - id: CONTRACT-001
      kind: governed_by
    - id: CONTRACT-003
      kind: governed_by
---

# R17 .51 one-shot local lifecycle proposal

**Status: consumed; pre-effect failure.** The exact R17 approval was used for one attempt and is spent. It ran for 6.203 seconds, then refused because the original selected PGDATA path no longer matched its frozen 11-file inventory. The attempt root remained empty; the supervisor records `phase: none`, `native_effects: none`, and `effects_may_remain: false`. No PostgreSQL start, migration, Spark, publication, ACK, seed, or continuation occurred. The original selected and cold-backup paths currently contain no files; the cause and time of that loss are unknown. The [R17 failure record](r17-pre-effect-failure-20261031.md) documents this outcome and identifies exact matching, separately retained R16 copies as recovery candidates. It grants no retry.

The [R17 scope proposal](full-lifecycle-scope-proposal-r17.json) keeps the selected local source, platform/runtime pins, migration route, bridge, publication flow, seed request, source/feed/consumer/checkpoint custody, and explicit `local_truss_acceptance_trial` origin approval wording. It uses a fresh attempt UUID, destination ID, output tree, supervisor tree, socket, restored PGDATA, discrepancy preservation target, registry path, and receipt paths. It does not reuse the R16 attempt output as an R17 target. The paths were observed absent when drafted; any later approval packet must recheck every path and source pin.

The exact proposal SHA-256 is `923f19c42617ed3d7053ba09d1125d698f07110f085c34eb78e2fd6d7ca2b798`. It binds the copied R17 prefix worker (`62340985b1beeecede16931649863891f35ce4a5936b38169b9277d236080461`), post-migration lifecycle worker (`43d46895fa0d77417cb2d2c71a4ce846c78486dd37b926d852bed9e85b171966`), and supervisor (`17a0e348beb2ebd061d8645e04c0612130debeafe85f9b8a81b706727728079e`). The source, runtime, bridge and semantic-origin fields are copied with their exact R16 pins; the R16 proposal and approval hashes remain separately asserted by inert tests.

## Time and resource bound

A 600-second total deadline would leave 294.405 seconds after R16's measured 305.595-second preparation. That is too little planned room for the complete one-start lifecycle after preparation. R17 proposes a 900-second wall-clock ceiling, leaving 594.405 seconds after that measured preparation. The planning estimate reserves about 300–450 seconds for PostgreSQL start, migration/readback, publication and ACK, seed, continuation and ACK, then clean stop. This is a planning estimate based on the observed preparation floor, not measured native duration and not a performance acceptance target. The 900-second bound is a 15-minute maximum for the entire one-shot attempt, including preparation.

The prefix worker and parent supervisor both enforce 900 seconds; the post-migration lifecycle checks the same approved ceiling and its alarm uses the scope value. The prefix retains the frozen helper's `ASHLAR_R16_SOURCE_SRC` environment key. The R17 scope binds registry-absence receipt SHA-256 `656c0eea7b9538d7c87e7610efc0131affa7cf9a3b1272de3798b4314be74b76`. The process inventory admits only when no `postgres` or `pg_ctl` process owns either PGDATA through `-D`/`--pgdata`; unrelated process command lines may mention those paths. The 2 GiB process-group RSS cap, 2 GiB aggregate retained-plus-attempt disk cap, 512 MiB attempt-tree cap, 16 MiB combined-log cap, one attempt, zero retries, and sequential local-only execution remain unchanged. Monitoring remains sampled; brief resource spikes between samples may go unseen, as recorded in the scope.

## Review and checks

Three R17 code files are copies of the corrected R16 implementation at main commit `5925ebd8`, with R17 identities and the consistent deadline adjustment. The frozen source archive, manifest, helper and all 569 selected source files pass the copied supervisor verifier under the pinned Python 3.12.14 runtime. Import-only Truss/Weft/Spark/Delta route and package checks pass; `pg_ctl`, Bun and both Delta jars match their pinned hashes. All 31 R17 inert tests pass (3 proposal/scope, 18 custody-prefix, and 10 lifecycle tests). They verify exact worker hashes, fresh scope identities, R16 historical scope and approval hashes, origin wording, the frozen helper environment variable, exact registry receipt digest, ownership-based process admission wording, the aligned deadline values, and unchanged 2 GiB RSS cap. Compilation also passed. These checks did not invoke PostgreSQL, Spark, migration, publication, ACK, seed, or continuation.

The R17 proposal is superseded for execution by its consumed pre-effect refusal. A future packet must have a fresh identity and bind the retained R16 copies plus their exact preservation receipts; the original missing paths cannot be used as inputs. Independent review and a new owner approval remain required. This proposal grants no further execution authority and makes no production-support claim.
