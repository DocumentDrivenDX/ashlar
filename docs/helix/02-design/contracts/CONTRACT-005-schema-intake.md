---
ddx:
  id: CONTRACT-005
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-003
    kind: informed_by
  - id: CONTRACT-004
    kind: informed_by
  - id: FEAT-002
    kind: informed_by
---

# Contract: shared UMF schema intake

Proposed ashlar-schema-intake/0.1. Intake validation is not catalog acceptance,
constraint enforcement, schema migration or source-data publication.

The development CLI tools/inspect_umf.ts accepts a trusted UMF source directory,
its exact Git revision and one UTF8 JSON schema document. The source checkout must
be clean, match the supplied revision and supply its existing reader/validator.
Refuse invalid UTF8, invalid UMF or a changed/unpinned validator source. Output
is one JSON artifact containing format, sourceBase64, sourceSha256, sourceBytes,
umfCoreVersion, documentId, validatorRevision and the original validation result.
Use the exact retained bytes as source authority; no reserialized document replaces
them. No private UMF vocabulary or alternative semantic validator is introduced.

Both downstream schema registries must verify the retained-byte digest before
consuming this artifact and perform their own versioned binding/catalog acceptance.
The artifact records validatedStructure and completeInterpretation separately.
Unknown assertions remain retained. A complete UMF validation still does not mean
Truss or Ashlar can enforce every assertion; target bindings must classify native,
engine and unsupported enforcement, and refuse unsupported executable ingestion.
Unknown/breaking schema effects cannot advance a published data revision silently.

A document revision identifier and stable catalog ID mapping must be supplied by
the selected schema registry/source profile, not inferred from this inspection.
Same schema identity/revision with different source bytes is a conflict; original
bytes remain authoritative on replay. Additive compatibility, key/relationship
changes and data conversion need explicit target-specific acceptance rules.

Raw intake artifacts MUST be retained independently from accepted catalogs.
Raw registration MUST NOT write Truss schema_doc, accepted catalog head or an
accepted Ashlar schema revision. Cross-machine distribution MUST independently
establish original producer identity and artifact custody.

## Raw registry surface

The proposed raw registry key is (document_id, document_revision), with explicit
opaque nonempty revision supplied by the caller. Its eight required fields are
document_id, document_revision, source_base64, artifact_base64, source_sha256,
artifact_sha256, validator_revision and complete_interpretation. Base64 retains
exact source and intake-artifact bytes; SHA256 is lowercase hexadecimal over
those decoded bytes. Preserve the full diagnostic artifact, including unknown
fields and validation warnings. validatedStructure and validation.valid must
both be true; completeInterpretation must agree with validation.complete.
The reader checks source identity/core version and exact byte length/digest.
Duplicate JSON members, nonfinite numbers and malformed UTF8 refuse.

An identical complete row replays without replacing the original. Any changed
field under an existing qualified key refuses SCHEMA_INTAKE_CONFLICT and leaves
the original intact. A later revision is a separate retained artifact, not an
automatically compatible or accepted schema. For example revisions 1 and 2 of
truss-weft-source-review-fixture retain the required label and additive optional
caption respectively; neither advances an accepted catalog or publication.

The caller must supply the trusted validator source pin and independently trust
the original artifact producer/custody. Comparing an artifact's declared pin
does not authenticate it or rerun UMF. This reader is a custody verifier, not an
alternative UMF semantic validator. No signature or remote intake authority is
claimed by the local development profile.

## Raw-store mutation and recovery boundary

Decoded-byte hashes and complete original source/artifact bytes MUST survive
native storage and replay. Qualified-key uniqueness MUST be enforced under the
selected concurrency profile. A serialized development Delta writer does not
establish concurrent uniqueness. Native constraints, application fencing and
privileges require independent verification; administrator authority MUST NOT
be presented as production immutable-registry enforcement.

Installation MUST refuse silent IF NOT EXISTS reuse. Failed or uncertain writes
MUST retain the original native statement handle and custody, inspect their
outcome and reconcile before any replay or replacement submission. A failed
post-mutation check MUST NOT be reported as rollback. Store registration remains
separate from accepted schemas, stable binding allocation, constraint enforcement
and runtime/feed operations.

## Stable identity planning boundary

The proposed pure planner accepts a complete selected identity inventory, complete
prior mappings, family highwaters and the expected native catalog head. Native
acceptance must read these under exclusion, revalidate the resulting plan and
persist allocation with the complete accepted semantic/catalog/report effects.
A returned plan is not a committed allocation or acceptance token.

Identity parts are exact opaque UTF8 strings; no case-folding or Unicode
normalization occurs. Type identity is (document, module, authored element);
property identity is (document, owning module, owning Record element, field
module, field element); proposed relationship identity is (document, owning
module, owning Record element, authored relationship). Display names, source
ordinals and document revisions are definition provenance, not lineage. The
relationship tuple remains subject to the selected Truss/UMF relationship
extraction profile; this planner does not extract relationships or admit
relationship semantics. Existing mappings must come from authoritative catalog
custody, never from the synthetic Weft source-review IDs.

Each family has a separate signed-int32 positive ID allocation range. Preserve
all prior IDs, including retired identities; allocate fresh IDs strictly above
the supplied highwater. Missing mappings do not authorize filling highwater
gaps. Duplicate identity/ID mappings, invalid ranges, incomplete highwater
inventory or active members without an active owning type refuse. A complete
inventory omitting an active prior identity plans retirement; same qualified
identity reactivation keeps its ID and never restores grants implicitly. A
distinct authored identity gets a fresh ID. Exhaustion refuses a new allocation,
but does not prohibit validation/reactivation of an existing identity.

The identity planner MUST NOT infer field meaning, key equality, schema
compatibility, data transforms or native effects. Its caller MUST admit those
under the target profile and supply complete selected ownership. Authored identity
projection MUST use the pinned UMF reader/validator, retaining every original
element tuple, source pointer, kind assertion and validation diagnostic. Such
projection MUST NOT infer executable type/property/relationship bindings or
replace atomic native semantic/catalog acceptance.

## UMF interpretation and selected binding planner

Host tool tools/inspect_schema_semantics.ts invokes the pinned UMF reader and
existing kind, nullability, cardinality, facets, keys, relationship and schema-
property interpretation APIs. The report retains exact source bytes/digest and
original complete inspection receipts. Unknown and legacy meanings stay as UMF
returned them; no consumer token alias or invented author receipt is introduced.
Each interpretation API MUST be qualified for its selected core and result
versions. A version refusal and its original error MUST remain explicit; missing
schema properties MUST NOT be presented as complete interpretation. Current 0.8
metadata selection and property inspection MUST retain their own original result
shapes; they MUST NOT be recast as historical 0.7 operation receipts.

Proposed ashlar-string-record-binding-plan/0.1 is a target consumer planning
profile for explicit core 0.7 singleton string Record members. It checks complete
original element receipt membership, source/identity/pointer correspondence and
digest before deriving owner-qualified type/property identities. Source kind
must be known Record/Field; selected Field scalar family is string, cardinality
is known one, and availability is known required or absent-allowed. Original
source and inspection bytes, names and all warnings remain retained. This
consumer requires independently trusted original interpretation custody: matching
a declared validator pin is not authentication or UMF receipt recomputation.

Unsupported kind/value shape, unresolved/duplicate member, unknown availability,
facets, keys, extension meaning, relationship or other unbound assertion blocks
this selected executable profile with source pointers and none enforcement.
Each supported member MUST distinguish intended engine checks from verified
database enforcement and retain the corresponding evidence.
No candidate plan grants data-write authority or advances schema/head/publication.
Broader values, keys and relationships require their own qualified bindings;
the singleton string profile MUST NOT replace those requirements.
The native accepted profile must compose their original semantic and effect
obligations before claiming complete runtime support.

Unknown nullability tokens and newer or unknown assertions MUST remain as the
pinned UMF APIs interpreted them. An independently authored replacement fixture
or later document revision MUST NOT reinterpret retained earlier source bytes.
Structural validity and presence of a source Field do not establish executable
binding or accepted catalog authority.

Raw registration MUST retain exact documents and complete receipts through the
selected immutable registry, with source identity, validator digest, original
source/artifact readback, writer policy and registry UUID/schema/grant admission.
Raw rows MUST NOT authorize data ingestion or advance Truss schema_head.

The StringRecordPolicy profile MUST enforce interpreted required/absent-allowed
singleton string Fields using complete explicit trusted catalog bindings.
Unsupported assertions, nulls, arrays, unknown properties and source/revision
drift MUST refuse. Native acceptance MUST prove supplied IDs/revisions against
retained catalog authority; fixture IDs never establish that authority.
Relationship, broader value/facet/key and native enforcement bindings remain
separate required profiles.

The intake-to-meaning barrier MUST reparse full retained intake custody and
require the interpretation’s exact source digest and pinned validator/revision
to match. A same-named source or altered receipt cannot instantiate the selected
policy. This does not admit caller-supplied catalog mappings as native IDs: Truss
acceptance report/head custody remains required.


## Registration command admission

The register_umf.py command MUST derive its namespace only after checking
complete original installation/model/generator custody under the same rule as
the native CSV runner. Its development owner lane MUST observe the current actor
and independent catalog/schema/table owners, then obtain complete bounded
paginated effective-permission observations, retaining raw pages before
interpretation. Unknown/partial/inherited writer meaning refuses. SHOW GRANTS is
not used as the complete inventory in this path. An admitted schema_intake table
MUST be Unity Catalog MANAGED. Registry UUID and exact schema checks remain
independent of these owner/permission observations.

The supplied document bytes MUST still equal the actual inspected source on
admission, before and after registration and before a retained summary is emitted.
A failure after a native mutation does not prove rollback; retain the original
journal/statement handle and reconcile without replacement submissions. The
writer remains a same-host cooperating lock under trusted administrators;
renewed observations are not remote fencing or accepted catalog authority.

Fresh registration and replay MUST verify complete original raw rows and retain
unchanged original mutation handles/request/response receipts. Full installation,
qualified uniqueness, hash constraints and executable catalog acceptance MUST be
independently evidenced. Existing publication journals MUST NOT switch registry
UUIDs without changing their original workload identity.

## Stored development schema-evolution source

The bounded native source runner accepts --source evolution for the existing
original v1-to-v3 string-Record example. Primary --intake-proof MUST match
revision 3; exactly one --additional-intake-proof MUST match revision 1. Both
complete original source/artifact fingerprints and validator revisions MUST
match the local retained intake artifacts. Both proofs MUST name the same
installed managed schema_intake UUID in this namespace; fresh native checks
compare every original raw row before source/publication admission.

Publication schema inventory explicitly names fixture revision 1 and fixture-v3
revision 3; these are development inventory aliases, not native catalog heads.
Both are required because immutable history and live entities can retain
independent schema revisions. No lexical version ordering or latest fallback is
permitted. The original local policy explicitly admits only the additive
object replace transition from 1 to 3. Revision-1 delete remains governed by
revision-1 policy. Actual UMF Record checks cover original values and prior
live values under revision 3 before planning the transition. Unknown meaning MUST remain retained; interpretation completeness MUST agree
with the original selected API result and MUST NOT be upgraded by this profile.

Evolution scope MUST retain both complete intake rows and their registry UUID
in its original workload digest. Checkpoints remain original JSONL byte offsets,
not schema versions or group ordinals. Native effect/history/publication/pin/
retention admission remains required. Native evolution qualification requires original terminal receipts and complete
inventories. This profile MUST NOT admit arbitrary migrations, accepted Truss IDs
or new schema transformations.


## Binding-derived logical Record requests

StringRecordPolicy.record_value_request(change) MUST admit the original source,
revision, type and property inventory before translating numeric IDs to the
qualified Record/Field identities retained in its original binding plan. It
returns identity and typed string values. Omitted properties remain absent;
retained_json remains opaque. It MUST NOT assume example IDs 17/23/24, infer IDs,
validate UMF independently, claim native acceptance or apply defaults.

Host helper check_bound_umf_records.check_bound_records groups original
non-delete records by their explicit qualified Record identity and invokes the
actual pinned UMF Record checker for each group. It MUST retain independent
complete original receipts rather than combine them into a fabricated producer
result. Binding source SHA and the reconstructed original intake MUST agree.
Every original complete transaction is admitted; deletion values still pass the
host binding policy but do not become a new logical Record value observation.
No publication or acknowledgement is issued by this helper.

### Binding-derived source preflight

The native development runner MUST obtain logical Record/Field requests through
its selected admitted StringRecordPolicy rather than fixture type/property IDs.
Every selected original intake/binding MUST agree on source bytes and document
revision; actual upstream receipts MUST identify the pinned UMF checker. Each
qualified Record retains its own unchanged original receipt.

Before a selected schema transition, retained current values MUST be decoded
using their original source/revision policy and matching original history. Their
logical Record/Field references may be checked against the explicitly selected
target definition; target IDs MUST NOT overwrite original IDs/history/revisions.
Missing original binding/history, source drift or absent target correspondence
MUST refuse. Unmapped retained Fields MUST NOT disappear. This is a logical
prestate check, not automatic compatibility, data conversion or native acceptance.

### Explicit configured JSONL input

`ashlar-jsonl-source/0.1` is a development host configuration, separate from native
Truss catalog authority. It requires explicit feed, epoch, sourceSystem,
schemaAlias, documentRevision, validatorRevision; exact path/SHA256 references
for source, original schema, intake and interpretation; and complete qualified
MappingEntry bindings (family, parts, catalogId, active). Relative paths resolve
against the configuration file. Unknown fields/versions, digest drift, missing or
foreign bindings and mismatched original schema/intake bytes MUST refuse.
Retained configuration and referenced bytes MUST be rechecked during native
custody. The configuration digest participates in the original publication
journal scope. It MUST NOT license another source/configuration to reuse that
scope, or confer accepted IDs, automatic schema evolution or source ACK.

The native runner's configured-jsonl path MUST use the admitted managed carriers,
source/policy validation, original phase/effect/artifact/manifest custody, retention
and resolver/pin checks. The selected profile remains singleton string objects.
Singleton queries use the configured source and explicitly bound type, with an
explicit --type-id when there is more than one Record type. No new IDs are inferred.

The installed configuration loader MUST expose
`ashlar.source_config.load_jsonl_configuration` with compatible host imports.
`inspect_configured_source(configuration)` MUST return local custody and
staging/apply/delete/history/exact-replay counts without applying SQL. The installed
`ashlar inspect-configured-source CONFIG` command MUST expose this inspection and
buffer its summary until complete file/replay and closing input checks succeed. Actual
upstream UMF logical-value validation remains the host preflight; local inspection
MUST NOT relabel retained artifacts as a fresh validator execution or publication.

## Installed Truss catalog preparation

`ashlar prepare-truss-catalog --config CONFIG` is an additive, effect-free host
boundary. It MUST call the selected installed Truss public
`prepare_acceptance(tuple[CatalogDocument, ...], configuration: bytes,
bun_executable=...)` API, whose result is `PreparedAcceptance`. The selected
protocol is `truss-python-umf-preparation/0.1.0`, producing original
`truss-acceptance-input/0.1.0` bytes. Preparation MUST NOT invoke a database,
source factory, ACK, accepted catalog, head, feed, readiness, migration or
publication port. Profile pins and producer provenance establish correspondence,
not authentication or installed policy authority. This boundary cannot satisfy
real Truss feed integration.

`TrussPreparationConfig` is an exact closed `ashlar-truss-preparation/0.1`
configuration. It owns an ordered tuple of one through 512 documents, each with
explicit document ID, revision, optional artifact identity and canonical absolute
regular-file path/SHA256; one original configuration file path/SHA256; selected
installed Truss distribution version, immutable source revision, wheel digest and
installed resource/origin descriptor; selected Python executable and Bun executable
path/digest/runtime version; a canonical private fresh output directory; and
explicit finite limits. No environment selection, credential discovery, checkout
import, mutable sibling source, implicit identities or inferred profile pins are
permitted. Duplicate members, booleans in integer limits, unknown versions or
fields, aliases, symlinks, changed originals and nonexclusive output refuse.
The Truss host requires its selected Python version at least 3.11; this MUST NOT
raise the portable Ashlar core's Python minimum or introduce Truss imports there.

Pass document content and configuration as their unchanged original bytes.
The declared UMF version is retained inside each source, never rewritten to fit
an available owner. The caller's configuration explicitly selects layout,
acceptance, validator, support, binding, policy and transforms under the public
Truss input contract; Ashlar MUST NOT invent these choices or validate UMF itself.
Retain every `PreparedAcceptance` raw byte view: input, declarations, archived
documents, report evidence, each original document and complete interpretation
observation, plus all provenance fields. Unavailable owner observations, residuals,
unknown content and complete refusal diagnostics MUST survive unchanged. Parsed
summaries MUST remain separate from these authoritative byte views.

Limits MUST be supplied before file reads, child creation or serialization:
input/request capacity at most 1 MiB, each original source within that aggregate,
producer/child captured stdout and stderr together at most 16 MiB, aggregate
released byte views and receipt at most 64 MiB, and finite child deadline at most
60 seconds. A lower selected limit remains binding and may refuse a valid larger
preparation. The selected public Truss owner additionally enforces its original
request, producer capture and runtime deadlines; this adapter cannot widen them.
Check the aggregate length of all returned views and representation expansion
before encoding/copying them into the child transport or release artifact; count
duplicate retained views as distinct output bytes. Bound raw capture before read
or decode, and bound JSON depth/tokens before allocating parsed summaries.
These are transport/evidence bounds, not an executor heap guarantee.

The installed host command MUST own a finite isolated child and its stream,
deadline, reap and closing checks. The child imports only the selected installed
package and calls only its public preparation API; no fallback exists. Renew
original input/configuration/runtime/package and private output-directory identity
at opening and independently during closing on success, error and cancellation.
Every acquired resource MUST receive independent cleanup attempts. Preserve the
original `BaseException` identity when capture, close, fsync, reap or closing
custody also fails. Release a bounded receipt only after accepted complete child
transport and closing checks; failure artifacts are explicitly incomplete and
MUST NOT acquire a prepared/accepted status. Partial output never licenses a
catalog or source effect.
