# Fresh original-commerce Fabric functional attempt plan

**State:** offline candidate plan only. A read-only authenticated portal
preflight observed the current principal and visible workspace/lakehouse/FTL4
trial assignment ([receipt](../commerce-fabric-readonly-live-preflight-20261023/README.md));
this was not execution of the planned attempt. No candidate OneLake request,
graph creation, load, activation, compute, upload, or cleanup action was
performed. This is not a native qualification receipt or authorization to launch.

This packet scopes one new bounded P06/P07 functional attempt against the
retained original-commerce source and the already observed Fabric Graph GQL
surface. It does not reuse the ambiguous upload candidates A or B, and it does
not treat their later 404 readback as proof of their original PUT outcomes.
The packet is governed by [CONTRACT-001](../../../02-design/contracts/CONTRACT-001-publication-boundary.md)
and [CONTRACT-003](../../../02-design/contracts/CONTRACT-003-delta-graph-tables.md),
especially its private release activation, original-commerce held deployment,
OneLake fresh upload, and native admission/query sections. The existing
[R04 plan](../../toolkit-delivery-plan.md#release-continuation--current-upstream-cohort)
and [read-only reconciliation](../current-upstream-release-20261021/fabric-held-readonly-reconciliation.json)
remain authoritative for execution scope and the exact A/B status.

## Frozen source and oracle references

R1 must be freshly re-admitted from the live original-commerce source and its
ordinary source/ACK owners at execution time. Retained local projections are
byte-level oracles only; they are not continuing source authority. The
historical R1 publication oracle is:

| Input | Pin |
| --- | --- |
| Original local publication | `original-commerce-publication-1`, release SHA-256 `878291819c574f7a7d32d09c92d4a4687b65d3ab8ebaeb639263cb58d31ad29a` |
| Four-role vector | nodes/current UUID `737e1efc-a0b7-46ce-a82c-00ab4d8a7d6e` v2; edges/current UUID `cfce0e22-faaf-4791-93d2-329e5d9d35fe` v2; tombstone UUID `a80f4f91-732b-4bc4-89c5-94a268c25230` v1; history UUID `d76f3bac-b71e-4f16-81be-3afc0c83cc99` v2 |
| Retained source/ACK record | Source signature SHA-256 `1b6ce3c1ac4096ae2f5d9faafd74e801e09a47115a7133f6640d08e9fecc7544`; original source request SHA-256 `8fde1b80674747810602d44893e176eabbfaef23673c58bd215b91dfcd8f6c93`; original manifest SHA-256 `e5684ca2d4f9f6471c09eca2b1b2d8be6422a294f4c00755865f0589c8a87ab3`; retained source checkpoint is feed `private-original-commerce-fixture`, epoch/batch `original-commerce-v1`, position `1` after `0`. These pin the historical observation and must be freshly re-admitted under live owners. |
| Held publication source record | `docs/helix/04-build/evidence/commerce-fabric-delta-native-20261016/report.json.gz`, SHA-256 `3d86d7a4e3a92b20dacaf0bcf0cb1dc80d92421da1b083ff5b1805eba9f50a90`; uncompressed report SHA-256 `81d7ccbe99cb531b9895711783eb8f2f6d520912f75d9287918bd153463228a7` |
| R1 canonical row oracle | `commerce-fabric-publication-gql-20261016/expected-rows.json.gz`, uncompressed SHA-256 `b5dd747b41062d3185d4f212b0bd24b468d48b5d744af9ba460e9fadd0231361`, 39,149 bytes |
| R1 GQL request corpus | Node `a3efe7864707d05428325ae83d78155ca81db18cfebf7c26cd0c7d86dd22cecc`; edge `0a553f05f171ccfb303e904cc4226d4f8782ec841153682a86dd290bce1a593f`; endpoints `780d73dc17ded266610d8665450ee235fb8736ba3786b21c2b267bdef03155ef`; node count `93a141cee2e18d2048955fdb2e2e6f7e00de4c47ca9f8d552165ee74df2d9078`; edge count `b6208d67a92cc1b19af2478df19179f162910ed63ff9469636c1113aa426b900`; distinct-edge two-hop `245313eff1b16d4b91da4196db0f95383a2cbe1529bbddb004f703d95683b75c`; explicit WALK `6c6734975cde6bb04acadd8781720044f48b61c5b5ee52e06e6af2d6706464d2` |
| R2 separately authored presence case | `commerce-present-field-preparation-20261009/candidate.json`, SHA-256 `450c82ef4b76fd73b095dc4617d0a9d2a9f9d06c15b9e402fc757be31b510aba`; decoded public source SHA-256 `bdd21b1566e66dbe5cb1fd2ff6e098853aa9766b4bf6cdb1cf5192ecd0191576`; original-source/model pins inside candidate are `51d87c554df36846e41378cfaafc81277fcab61f6c891a6c64f34dba8fd9ac2a` and `8052dbcf2cf48a5e344c492d661872bc0a3f80a5c7be033ab5008dea9c86119e` |

The R2 presence candidate preserves the original 11-node/10-edge graph and adds
one separately authored product (`ashlar-added-product`) with String
`products.name = "Added presence control"`, String
`products.evolution_note = "Explicit newly authored String"`, and its required
supplier relationship to source product S1. It is a development preparation,
not an accepted source transaction, UMF admission, publication, or ACK. Before
any launch, an authorized current source/ACK owner must admit the exact R2
transaction and produce a fresh complete publication vector. If no such source
transaction is available, stop: do not synthesize R2 directly in a Fabric
carrier and do not infer native R2 authority from this candidate or old exports.

The successful 2026-10-16 native Fabric receipt remains historical evidence
only: it loaded/query-checked one R1 release, using graph item
`c974e2ff-8bdb-4653-a007-1d213882d5c5`, model creation operation
`92929d60-d8a4-43e0-ae96-50856b12c428`, and prior table UUIDs
`24dab8a3-e377-4a3b-8187-0facdb608bcd` / `c45941c5-0b10-4fc9-b032-fc7526c5d952`.
None of those model, operation, table, release, or source-path identities may be
reused by this attempt. The earlier held upload attempts A and B likewise keep
their own candidate IDs and remote paths permanently; the Oct 21 read-only 404
does not authorize retrying them.

## New candidate identities

The following identifiers were generated offline for this plan only; they are
not reserved remotely. The launch operator must verify that each exact table
path and display name is absent using read-only observations. Any collision
invalidates the packet and requires a newly reviewed candidate before writes.

| Purpose | Fresh candidate | Intended table names |
| --- | --- | --- |
| R1 | `70cca0ae-e0db-431c-81e9-6651a43f74e7` | `Tables/dbo/ashlar_commerce_r1_70cca0aee0db431c81e96651a43f74e7_nodes`; `Tables/dbo/ashlar_commerce_r1_70cca0aee0db431c81e96651a43f74e7_edges` |
| R2 | `4d559fd9-ac44-462b-b208-55826d9ad35a` | `Tables/dbo/ashlar_commerce_r2_4d559fd9ac44462bb20855826d9ad35a_nodes`; `Tables/dbo/ashlar_commerce_r2_4d559fd9ac44462bb20855826d9ad35a_edges` |

Create one new per-run journal under a new private run directory, for example
`/private/tmp/ashlar-commerce-fabric-r1r2-20261023-70cca0ae-e0db-431c-81e9-6651a43f74e7/journal.sqlite`.
The exact run directory must be checked absent before initialization and retained
with its candidate intents until the evidence packet is complete. It is not an
existing A/B journal and must not be recovered from or copied over them. The
workspace, lakehouse, principal, storage account and current capacity must be
freshly selected and recorded at preflight; historical workspace IDs are not
assumed current by this packet.

## One bounded attempt sequence

1. **Read-only admission.** Pin the committed/reviewed runner revision and
   artifact hashes; confirm the selected delegated account and workspace from
   current passive configuration; read the existing active capacity and lakehouse
   identity; check all four new candidate paths and display names are absent.
   Resolve each candidate into its complete GUID-form OneLake URI using the
   freshly read workspace and lakehouse UUIDs. Before upload, freeze each exact
   source-selection inventory as canonical compact JSON over sorted relative
   paths, lengths and SHA-256 values, and retain its inventory digest beside the
   corresponding R1/R2 intent. Names in the candidate table above are relative
   names only; no write is permitted until both complete URIs and inventories
   match the exact live source release.
   Do not provision, restart, resume, or change capacity. Verify in the live
   account that the already-active FTL4 free-trial capacity remains free for
   this exact work. The candidate hard cost bound is **$0**: no paid capacity,
   provisioning, upgrade, or billing fallback is allowed. If current service
   state cannot establish that zero-cost bound, including startup and cleanup,
   stop without writing. The separate $10 preferred/$100 hard bound in the
   delivery plan applies to Databricks activity; it does not authorize Fabric
   spend. No Fabric estimate is inferred from Databricks DBU pricing.
2. **R1 source hold and immutable staging.** Open the exact original-commerce
   source/ACK/native interval. Re-admit the live source and compare its complete
   release, manifest, request, native version-zero UUIDs, ordered schemas,
   complete row occurrence bags and four-role publication vector with the
   frozen R1 pins above. Retain the exact complete source/ACK interval across
   the release projection and all owned local work. Materialize fresh complete
   node/edge projections and verify their full bags. Use only the new R1 paths,
   per-path `If-None-Match: *`, flat Parquet plus one Delta version-zero commit,
   bounded bytes, and full readback before model creation. Never overwrite,
   delete, retry, or recreate a path.
3. **R1 creation, load, admission and queries.** Submit exactly one create
   request and durably record its original response/operation handle before
   observing it. Record the actual portal/editor initialization owner and its
   opening/closing evidence; a callback or Boolean is not a substitute. Observe
   only the original operation and its sole initial-load handle to terminal
   completion; no refresh fallback. Require supported definition readback and
   exact mapping and locations, then independently compare complete native
   node/edge/endpoint result bags to the R1 oracle. Run the seven pinned queries
   once each and retain full bounded responses. Admit/activate R1 only while the
   actual source/ACK owner and private mutation/read leases remain valid. Verify
   active pointer equals the exact R1 candidate, item, release, mapping and
   vector.
4. **Interrupt inactive R2 after nodes.** Admit the fresh R2 source/ACK interval
   only if its exact source transaction is independently accepted, then reserve
   its distinct journal intent and paths. Stage and read back the R2 node
   projection only, and deliberately stop before writing the R2 edge projection
   or submitting graph creation. Verify active pointer remains R1. Query R1
   using its actual owners or record an explicit refusal. This retained
   interruption witness must prove no mixed activation.
5. **Resume the same R2 intent.** Reopen only the original R2 intent after
   re-admitting the same source publication, vector, schemas and staged-node
   bytes. Do not regenerate IDs or rewrite staged paths. Stage/read back R2
   edges once, require endpoint validity and full-bag readback, then submit one
   new R2 graph creation with its original operation handle and sole load. Run
   the same seven queries against R2 and compare complete bags to independently
   produced R2 expectations: R1 rows remain unchanged and the added product and
   supplier edge appear exactly once with exact String values. Check definition,
   mapping, source paths and complete R2 vector before activation.
6. **Atomic activation and race witness.** Compare-and-replace expected R1
   active pointer with R2 in one durable local operation. A reader racing this
   activation must acquire the whole R1 or whole R2 release under a live lease,
   or refuse; no mixed result is a pass. Re-read both immutable R1 and active
   R2 through their actual source/ACK/native owners and compare exact identities,
   multiplicities, endpoints and values. If the source interval or lease cannot
   remain valid, record refusal rather than query from archived copies.

## Ambiguous-write reconciliation and stop rules

After any timed-out, disconnected or otherwise ambiguous OneLake create-path
PUT, append, or flush, preserve the original durable request intent and issue
**read-only GET/HEAD observations of that exact path only**. Never resend the
PUT, repeat an append offset, repeat a flush, overwrite, delete, or recreate the
path. A readback that returns every original expected byte with the exact size
and hash may close that path's byte correspondence, but does not by itself
prove completion of the table upload, source admission, or graph release. A
missing path (including 404), partial content, hash mismatch, unreadable path,
changed source intent, lost original journal, or unavailable owner remains
unresolved: stop this candidate and do not continue creation or activation.
For an ambiguous GraphModel create response, never submit another create. Use
only the original retained operation ID/location if the response yielded one;
if no original handle was durably retained, the result is indeterminate and the
candidate stops without catalog-listing inference or another submission. An
original known operation may be observed read-only until its terminal state;
unknown state remains a refusal. Preserve partial remote output and report the
exact request, response, journal state and read-only observations. This new
recovery procedure does not alter A/B custody or their historical ambiguous
outcomes.

Stop before further effects on any capacity/permission change, unowned resource,
unexpected item/job/refresh, duplicate or missing operation handle, unsupported
definition readback, changed source/ACK record, incomplete close, result-bag
mismatch, stale activation pointer, request/response/byte/deadline limit, cost
bound uncertainty, or failed cleanup. No automatic retries or fallback refresh.
An unplanned native graph deletion or OneLake cleanup is not authorized: retain
all remote objects and immutable staged bytes for inspection. Close and erase
local delegated tokens, close all source/ACK/HTTP/reader leases, stop only work
whose original handle is known, retain the journal and evidence, and verify
owned local processes have exited. A cleanup failure withholds a success result.

## Finite envelope and retained evidence

This is one sequential attempt, concurrency one, one R1 and one R2 item, at most
two table uploads per item, two initial loads total, seven exact GQL queries per
item, no refresh requests, and no batch/replay/stress matrix. The proposed
numeric envelope is: 900 seconds total process wall time including cleanup;
60 seconds per ordinary HTTP operation; 15 seconds for delegated CLI auth;
32 authenticated storage requests and 8 MiB total uploaded bytes per release;
1 MiB per file; 8 MiB per table; 65,536 bytes per HTTP request; 1 MiB per HTTP
response; 16 MiB aggregate upload bytes and 64 storage requests across R1/R2;
65,536 bytes per GQL request; one page and at most 100 rows / 1 MiB per query;
14 MiB aggregate GQL response bytes; and $0 total cost. Historical R1 files
total 34,186 bytes across four files, so the per-release upload cap is more
than 200 times that observed payload; this is a sizing reference, not a
guarantee for a newly admitted projection. Any exceeded cap stops the attempt
and preserves evidence. Preflight must verify these caps fit the selected
runtime and endpoint; any requested increase makes this packet stale and
requires owner review before launch. The cost cap remains $0 even if every
byte/time limit is met.

Retain the committed source revision and file pins; selected account/workspace/
capacity/lakehouse readbacks with private credentials removed; exact R1/R2
source and ACK receipts; canonical request and compact intents; all four-role
vectors; projection inventories and complete row oracles; per-file upload
request/response/readback hashes; create intents/responses and original
operation IDs; sole initial-load observations; exact definition; complete GQL
requests/responses and result-bag comparisons; R1/R2 activation receipts and
race result; terminal status, elapsed time, byte accounting, actual usage/cost,
cleanup verification and stop/refusal evidence. Capture any client-side runtime
warnings only in bounded credential-checked diagnostics. Mark absent authority,
unsupported APIs, delayed billing telemetry, or incomplete cleanup explicitly;
do not claim production isolation, continuous native snapshot exclusion,
numeric scalar promotion, scale performance, or successful R2 when those checks
were not actually made.

## Current launch blockers

This plan is not launch-ready. The 2026-10-23 read-only portal preflight observed
the current principal, workspace, lakehouse and visible Fabric Trial FTL4
assignment; it did not establish the exact attempt's all-in $0 cost or billing
fallback status, and capacity-admin details remained unavailable. The four new
candidate OneLake paths have not been checked for absence. Numeric Fabric
byte/time bounds and endpoint fit remain unverified. R1 source/ACK authority has
not been re-admitted, and R2 has no accepted live source transaction or fresh
source/ACK authority. The installed `OriginalCommerceSourceOwner.append_original`
in `src/ashlar_host/commerce_publication_sources.py` admits only the exact
driver-owned 21-record R1 batch on `private-original-commerce-fixture` /
`original-commerce-v1` and its first position (or exact readback at position 1);
it cannot append a successor R2 transaction. R2 therefore requires the original
source/ACK owner to accept and commit an exact successor transaction and provide
fresh source/ACK custody and a complete publication vector. The authored
`commerce-present-field-preparation-20261009/candidate.json` remains development
preparation and cannot serve as source acceptance or authority for an adapter.
The old Fabric success receipt covers only R1, and the old
A/B path reconciliation neither authorizes retries nor proves the new paths
available. Resolve these prerequisites and obtain independent review of the
committed runner/packet before any native call.

The held runner has a separate unresolved portal/editor initialization owner
gate. `HeldCommerceFabricDeployment.observe_initial_load` in
`src/ashlar_host/commerce_fabric_deployment.py` only lists and reads the item's
Refresh job; its contract explicitly says it never opens the portal. No
production implementation or nominal external owner for the first editor
opening, retained through its closing evidence, exists in the current runner.
The loopback deployment tests exercise REST job observation and therefore do
not establish this portal operation. Historical R1/R2 evidence in the
[release manifest](../fabric-delta-releases-20261011/manifest.json) says R2 editor Save reported
invalid elements and later showed data loading; the receipt leaves initial
ingestion completion pending and does not provide the actual portal owner's
opening/closing record. This does not satisfy CONTRACT-003's explicit portal
ownership requirement. No effect-free implementation can truthfully close it
without a supported/approved way to own and evidence that UI operation. Keep
P06/P07 held until the owner interface and exact opening/closing receipt are
available, then bind that actual owner across observation and load admission;
a callback, Boolean, job listing, or historical editor note is insufficient.
Microsoft's [Graph data management documentation](https://learn.microsoft.com/en-us/fabric/graph/manage-data)
states that REST-created models are not queryable until first opened in the
portal editor, which provisions internal loading infrastructure; refresh fails
before that one-time initialization. The [GraphModel REST operation index](https://learn.microsoft.com/en-us/rest/api/fabric/graphmodel/items)
lists create, delete, query, get, list and update operations, but no first-open
initialization operation. These public surfaces support keeping the owner gate
external to this runner until Microsoft exposes a supported initialization
operation or an authorized UI owner can retain actual opening/closing evidence.
