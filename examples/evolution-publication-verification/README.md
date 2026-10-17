# Verify an existing evolution publication

Use this workflow to check one already completed ordinal, `0` through `7`, of
the retained two-source, eight-publication commerce evolution run. An older
ordinal can be selected after later publications complete. Verification requires
current admission of the entire retained ledger and both ordinary source/ACK
owners. It compares all four materialization bags to the original source-prefix
union and validates complete native schemas under the selected publication's
immutable pins.

Use the installed Ashlar package and the existing qualified local
Spark/Delta/psycopg runtime. Select the original tables' schema profile explicitly:
use `"native_schema_profile": "canonical-managed-carriers/0.1"` in the invocation
for packaged UMF-generated carrier fields, including their declared nullability.
Omission preserves `legacy-sql-bootstrap/0.1`, whose primitive fields are nullable.
Both profiles require empty field metadata and apply to all four graph roles;
the verifier does not infer a profile or repair mismatched tables. Canonical
receipts identify the profile and generated-carrier digest.
Preserve the original six target identities, installation reservation,
native journal, run reservation, producer and two source registrations.

Copy [invocation.template.json](invocation.template.json) to a private operator
file. It explicitly selects the canonical profile; select the legacy profile only
for an installation originally created with that physical schema. Replace every `REPLACE_WITH` value with canonical absolute original paths
or the original run SHA256. Select the completed ordinal and explicit capture
limits. The receipt must be fresh and distinct from the configuration, provider,
ledger and installation files. The numeric limits in the template are explicit
example selections; they are not implicit defaults or a completion-time promise.

The trusted provider must export `open_verification(invocation)` and yield the
exact `VerifyCommerceEvolutionConfig` from
`ExistingEvolutionOperator.open_verification`. The
[existing operator example](../existing-evolution-operator.py) supplies this
binding. Explicitly supply its named `ASHLAR_*` installation, six-target,
ordinary source/ACK and cached-JAR inputs. The example uses one local worker,
one Delta snapshot partition and one shuffle partition for finite development
tables; select the operation capacity actually
recorded in the original installation. Its environment inputs construct the
operator, while the JSON file owns ordinal, producer, bounds and supervision.
Authenticate each ordinary role directly.

Run the following installed command inside an operator-owned process supervisor
matching the invocation's deadline and stream-capture bound. That supervisor must
terminate and reap its process tree on deadline or cancellation, retain bounded
stdout/stderr with explicit loss reporting, and retain the terminal exit status.
The command and example provider do not supply a watchdog. Set
`ASHLAR_PYTHON`, `ASHLAR_VERIFICATION_CONFIGURATION`, `ASHLAR_VERIFICATION_PROVIDER`
and `ASHLAR_VERIFICATION_PROVIDER_SHA256` explicitly; the last value is the
SHA256 of the exact trusted provider bytes.

```sh
"$ASHLAR_PYTHON" -I -B -m ashlar verify-commerce-evolution-publication \
  --configuration "$ASHLAR_VERIFICATION_CONFIGURATION" \
  --provider "$ASHLAR_VERIFICATION_PROVIDER" \
  --provider-sha256 "$ASHLAR_VERIFICATION_PROVIDER_SHA256"
```

Success requires the receipt, zero command exit status, successful supervisor
cleanup and all current closing checks. The receipt identifies the original run,
selected publication, complete four bags and schemas. Its
`selected-completed-publication-only` qualification does not prove completion of
the eight-publication run. Verification never prepares or resumes an unfinished
ordinal, repairs missing custody, appends source data or advances ACK/progress.
Retain a refusal's diagnostics and original installation; investigate before
another invocation rather than increasing limits or reconstructing missing
journals.

The owning desired behavior is [TD-003](../../docs/helix/02-design/technical-designs/TD-003-evolution-publication-verification.md)
and [CONTRACT-001](../../docs/helix/02-design/contracts/CONTRACT-001-publication-boundary.md).

## Read one identity from both source scopes

Use the same existing-publication verification configuration and explicitly
selected trusted provider, with a fresh receipt path. Inside the same finite
operator-owned process supervisor, run:

```sh
"$ASHLAR_PYTHON" -I -B -m ashlar read-commerce-evolution-singletons \
  --configuration "$VERIFICATION_CONFIGURATION" \
  --provider "$TRUSTED_PROVIDER" --provider-sha256 "$TRUSTED_PROVIDER_SHA256" \
  --kind object --type-id 16 --entity-id 3
```

`--kind` selects `object` or `edge`; both integer identifiers are signed 64-bit
values. The command reads that same identity independently in both registered
source scopes, including absence, under the selected completed publication's
actual provider, native snapshot pins and ordinary source/ACK reconciliation.
It writes a fresh private receipt only after provider cleanup and input custody
checks. It never publishes, advances a source or submits an ACK. The singleton
profile qualifies the selected complete rows and their original-source parity;
it does not inherit the separate verifier's canonical schema/eight-bag claim.
Command completion requires terminal 0 and its bounded stdout receipt; private
intermediate files alone do not establish completion.

## Start a fresh three-publication prefix

The installed command composes publication; it does not create PostgreSQL roles,
outbox/ACK services, Delta tables or a runtime. First supply an operator-owned
fresh six-target installation and two distinct ordinary source/ACK registrations.
The native graph tables must match the selected canonical field schema; retain
actual observed UUIDs, paths and original installation reservations. Seed all four
original source groups per source through their writer roles: admission retains
the complete original schedule even when only its first prefix will execute.
This requires a new installation and consumers, rather than adopting or resetting
an existing consumer. The provider's native/source/ACK owners must admit them.

Keep the complete installation, native journal, run reservation, run ledger,
original producer inputs and all six Delta directories under a durable,
operator-owned workspace location. Retention must preserve the data files needed
by later selected old-publication reads. An empty replacement table or copied
journal without its original native files cannot reconstruct this authority.

Copy [fresh-invocation.template.json](fresh-invocation.template.json) to a private
configuration. Replace all named selections and choose explicit UTC transaction
clocks and recording microseconds for the complete eight-publication request.
The clocks in the template are examples. `source_order` must match the two
registered feeds in A/B order; all eight publication IDs must be distinct.
The fresh ledger must share the original native journal/reservation parent
selected by the provider. Its receipt path must be fresh and distinct.

Set `ASHLAR_EVOLUTION_CONFIGURATION`, `ASHLAR_EVOLUTION_PROVIDER` and
`ASHLAR_EVOLUTION_PROVIDER_SHA256` to the selected private configuration and exact
[existing operator provider](../existing-evolution-operator.py) bytes. Supply all
of that provider's named `ASHLAR_*` inputs explicitly: six original target
registrations, native journal/run reservation, operation capacity, cached JARs,
and separate A/B ordinary source/ACK credentials. Use the installed runtime and
the bounded supervisor described above; keep credential files private.

```sh
"$ASHLAR_PYTHON" -I -B -m ashlar publish-commerce-evolution \
  --configuration "$ASHLAR_EVOLUTION_CONFIGURATION" \
  --provider "$ASHLAR_EVOLUTION_PROVIDER" \
  --provider-sha256 "$ASHLAR_EVOLUTION_PROVIDER_SHA256"
```

`through_ordinal: 2` publishes A1 → B1 → A2, completing three original slots and
advancing only the corresponding source ACKs to A2/B1. It leaves five slots
unprepared. Success requires a prefix receipt, zero command status, full closing
and confirmed supervisor cleanup. The receipt contains `run_sha256`,
`completed_count: 3`, `total_publications: 8`, `run_complete: false` and exact source
progress. Omit `through_ordinal` for the existing full-eight run; JSON null and
Boolean selections refuse. A prefix is not full-run qualification.

After a successful prefix, use its original `run_sha256` and a fresh receipt path
with the continuation command below, selecting ordinal **3** for B2. Continue one
next ordinal at a time through 7 without repeating fresh initialization, reseeding
or resetting consumers. After interruption, preserve all original custody and
establish the actual retained/completed prefix before selecting its permitted
continuation; absence of a final receipt does not prove absence of effects.

To verify A1 after B1 or later publications, select ordinal **0** in the canonical
verification template, using the same original run digest, ledger and provider.
That read compares the original A1 union and pins, while renewing current whole-run
and ordinary source/ACK admission. It does not publish or advance source progress.

## Continue an unfinished run one step at a time

`ashlar continue-commerce-evolution` resumes the original retained run through
one selected ordinal. Use the same installation, provider and ordinary source
registrations described above. The provider exports `open_evolution(invocation)`;
the existing operator example supplies both entrypoints.

The private configuration has this shape; replace the example paths, digest and
producer limits with your original selection:

```json
{
  "profile": "ashlar-commerce-evolution-invocation/0.1",
  "mode": "continue",
  "ledger_path": "/absolute/path/run.sqlite",
  "receipt_path": "/absolute/path/new-continuation-result.json",
  "producer": {
    "source": "/absolute/path/original-umf-producer",
    "bun": "/absolute/path/bun",
    "git": "/absolute/path/git",
    "timeout_seconds": 60,
    "maximum_output_bytes": 1048576,
    "maximum_receipt_bytes": 4194304
  },
  "expected_sha256": "REPLACE_WITH_ORIGINAL_RUN_SHA256",
  "through_ordinal": 2
}
```

If ordinals 0 and 1 are complete and ordinal 2 is retained, selecting 2 observes
the completed prefix and resumes its original transaction. It stops before
ordinal 3. Preserve the ledger and pending transaction bytes after interruption;
completion of a prefix does not prove completion of all eight publications.

```sh
ashlar continue-commerce-evolution \
  --configuration /absolute/path/continue.json \
  --provider /absolute/path/existing-evolution-operator.py \
  --provider-sha256 REPLACE_WITH_PROVIDER_SHA256
```

Run within the same bounded process supervisor used above. The producer's
`timeout_seconds` bounds its subprocess only; it does not bound the Spark
workflow. A successful continuation publishes and advances the corresponding
durable source ACK, whereas the verification command only reads.
