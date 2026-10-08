# Physical layout milestone acceptance

Owner-revised scope2026-10-08: accept the table-design/schema-package milestone
and move to implementation. The revision changes acceptance, not the historical
measurement results. CONTRACT-001/002/003 correctness requirements remain intact.

| ID | Required milestone outcome | Existing evidence | Result |
| --- | --- | --- | --- |
| LAYOUT-AC1 | Concrete UC Delta tables and scoped native structural evidence | CONTRACT-003 native0.3 CREATE/description evidence; packaged13 CREATEs; offline source/file hashes | Met for candidate package; extracted files not newly deployed |
| LAYOUT-AC2 | Preserve exact typed keys, property tokens, retained unknown content and related history | R694237 final statements/640 independent groups:16M edge/raw/adjacency and64M property events; complete carrier/raw/journal parity | Met for named synthetic pins |
| LAYOUT-AC3 | Preserve typed endpoint integrity and expose enforcement gaps | R69432M endpoint references resolve at old-node6/new-node15; package enforcement matrix and CONTRACT-003 | Met for synthetic checks and documented responsibilities; no native FK/fence claim |
| LAYOUT-AC4 | Reproducible completed publication boundary | Complete seventh publication N6/E10/R7/J7/A8/T7 and R602 disposition; immutable role versions/validation readback | Met for private serialized synthetic publication; production concurrency separate |
| LAYOUT-AC5 | Native singleton path independent of Fabric, with measured limits | R629/R633 full-carrier/absence responses and timings; packaged bound full-key/hash read templates | Met for feasibility;250/100ms targets not achieved |
| LAYOUT-AC6 | Scoped graph mappings and implementable handoff | CONTRACT-003; release mapping profile; table-design handoff; selectable SQL package | Met for mapping/design; external UC/Fabric deployment not implied |

The quick [offline package review](out/schema-package-review-20261008.json)
checks all package hashes, source hash,13 unique CREATE memberships and the
terminal preservation/endpoint receipts. It runs no SQL, Spark or benchmark.
This milestone is closed in this stated scope; it is not full Ashlar acceptance.

## Qualifications carried to later work

Warm engine100ms/caller250ms, controlled cold1s and freshness60s remain provisional
operational targets. Broad warm reads and complete publication measurements miss
their targets; controlled cold/service p95, sustained10k/s,100k/s burst and1B/5B
runtime are unproved. Keep these records; do not convert them into passed tests.
They do not block this design milestone or require another scale run.

Production source completeness/authority, serialized or fenced writes, crash/
concurrent recovery, effective caller policy, retention and engine-specific native
releases require evidence before their support claims. Their later acceptance
criteria are not waived. UMF binding remains deferred.

## Next step

Begin the publication resolver slice using the SQL candidate package and existing
CONTRACT-001/002/003: resolve one named immutable descriptor, validate its role
UUIDs/revisions/versions and effective caller policy, then execute the native
singleton template at that pinned version. Define failure/refusal behavior through
those contracts. Add a descriptor read template now; language/transport-specific
runtime and publisher deployment remain separate implementation choices.
