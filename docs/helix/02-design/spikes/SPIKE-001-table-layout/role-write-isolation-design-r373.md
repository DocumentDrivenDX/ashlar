# Role writer isolation and physical layout r373

Draft physical/publication design under CONTRACT-001–003 and proposed ADR-001. Uses the existing owner-selected Unity Catalog Delta architecture and the provisional performance targets; no source authority, production concurrency or1B/5B profile is approved here.

## Proposed layout decision

Keep stable canonical table UUIDs for object_current, edge_current, source_record, property_journal and tombstone, with optional adjacency/degree projections. Current object/edge tables retain typed native identities, exact property/retained/cursor text and lookup_hash liquid clustering without explicit partition directories;64MiB is an initial target, not a maximum. Raw and journal remain independent permanent semantic history. Typed graph releases remain independently bound to exact canonical versions. UMF binding remains deferred.

Do not require a fresh set of shallow clones for every source batch. Keep private attempt tables as an experimental/recovery isolation tool, with explicit parent pins and retention obligations. A production attempt-isolation profile remains unqualified; it cannot be silently substituted for a proven writer fence or advertised as a freshness solution.

The saved native r332 fixture attributes four serialized clone calls to22.384s, configuration calls to6.140s, empty tombstone/manifest creation to3.828s and first-batch fixture reconstruction to18.317s. Full preparation is110.835s and processing243.800s. These categories are measured separately; their sum is not the whole pipeline and first-batch reconstruction is not a required steady-state cost. Clones report36,923read bytes/0write/0spill, which does not measure metadata storage, checkpoint work or monetary cost. [Reproducible attribution](out/role-isolation-cost-r373.json) includes native-final IDs and source hashes.

At10k changed entities/s, a100k batch completes its arrival window every10s. Repeating only the measured serial clone sum yields a4,468entities/s service sensitivity before applying any changes. This is not sustained capacity, a lower bound for alternative implementations, or proof that parallel cloning would help. The evidence excludes clone-per-batch as an admitted default. The r26814.169TB active/211,134ideal64MiB-file/24B-bootstrap-journal arithmetic excludes retained files, logs, duplicate attempts and serving copies. Do not multiply a clone's low I/O counters into a billion-scale affordability claim.

## Writer admission and takeover

A publication authority owns one logical writer lane over all required roles. Its monotonically issued token guards descriptor advancement atomically in the authority/control table. The r369–r372 tests qualify seeded reference transitions only. They do not prevent a stale role write. The integrated profile MUST therefore refuse takeover until it has authoritative evidence that the previous writer cannot submit more role operations and that every already-submitted handle is terminal. A token comparison before a separate-table MERGE is insufficient because ownership can change between check and commit.

Before a new writer is admitted, record the prior worker/job identity, termination receipt, all submitted native statement IDs and terminal outcomes, last immutable complete vector, and every role's UUID/schema/profile/commit custody. Unknown submission outcomes retain recovery state; do not advance the generation based on timeout or elapsed lease duration. A cooperative local mutex, max-concurrency setting or process heartbeat alone does not establish that all old native work has ended. No automated lease expiry is selected.

The actual worker execution/control mechanism and its ability to prevent future submissions are unresolved. Until qualified, stable-table production writer admission remains blocked as a support claim; closed private fixture lineage remains the evidenced spike profile. This is not permission to provision/change access controls or claim a security fence.

## Exact commit barrier

For every modified role, enumerate the complete commit interval from its previously qualified base through the selected candidate version. Bind every clone/configuration/mutation/maintenance commit to its actual approved statement ID and expected operation/schema/profile. Confirm native terminal results, complete changed/current/history carriers, inherited custody, uniqueness/deletions and typed endpoints before advancement. A known token in descriptor text does not make an unknown role commit eligible.

| Stale-write timing | Required outcome |
| --- | --- |
| Before a selected role version | Unknown commit is inside custody interval: refuse publication, retain last complete vector and enter recovery. Do not filter out the commit or repair the receipt. |
| After a selected validated version | Existing pinned publication still reads its immutable snapshot. Record later physical history separately; no silent head substitution. The next writer must account for this commit before selecting a successor. |
| Stale descriptor attempt after handover | Native authority guard refuses the entire authority/descriptor MERGE; no source ACK. |
| Native statement outcome unresolved | Recover the same handle/attempt. No duplicate submission or generation takeover. |

If shared physical heads are contaminated by an unexplained write, quarantine successor admission. Requalification may require complete predecessor/output reconstruction or a new private branch from the last validated vector. This is an explicit availability failure, not a partial publication or a way to meet freshness by omitting checks. Native shallow-clone-of-clone limitations already observed in this workspace prevent assuming arbitrary branch chains will work. Recovery selection must preserve every carrier and raw/journal origin before publication.

## Next executable slice

Qualify the worker-termination/native-handle barrier and inject a stale role commit around the descriptor boundary on an owned private fixture. Show that an earlier pinned reader remains exact, an unexplained commit before candidate selection refuses advancement, and a verified owner can recover without rewriting source progress or discarding history. Record full input/change/custody checks and interference costs. If the execution mechanism cannot prevent further submissions, return to explicitly measured attempt isolation rather than pretending the authority-row guard protects role tables.

Integrate the selected lane with the liquid-clustered full-role publisher before another sustained/burst test. At that point, measure admission/recovery overhead inside publication freshness, not outside the clock. Keep100msengine/250mscaller,1scold,60sfreshness at10k/s plus100k/s burst and1B/5B obligations unchanged. Native graph mapping limits remain separate; no new PuppyGraph, GraphFrames or Fabric support claim follows.

The [native stale-role barrier](stale-role-disposition-r375.md) now qualifies one unapproved UPDATE refusal on a private40M-edge clone, with unchanged parent manifest and complete20-field old-pin equality for the injected carrier. It does not qualify worker termination, a concurrent cross-table fence or recovery of the contaminated head.
