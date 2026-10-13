# Verify an existing evolution publication

Use this workflow to check one already completed ordinal, `0` through `7`, of
the retained two-source, eight-publication commerce evolution run. An older
ordinal can be selected after later publications complete. Verification requires
current admission of the entire retained ledger and both ordinary source/ACK
owners. It compares all four materialization bags to the original source-prefix
union and validates complete native schemas under the selected publication's
immutable pins.

Use the installed Ashlar package and the existing qualified local
Spark/Delta/psycopg runtime. The selected tables must have the original local
bootstrap profile: fixed primitive columns, nullable fields and empty field
metadata. Preserve the original six target identities, installation reservation,
native journal, run reservation, producer and two source registrations.

Copy [invocation.template.json](invocation.template.json) to a private operator
file. Replace every `REPLACE_WITH` value with canonical absolute original paths
or the original run SHA256. Select the completed ordinal and explicit capture
limits. The receipt must be fresh and distinct from the configuration, provider,
ledger and installation files. The numeric limits in the template are explicit
example selections; they are not implicit defaults or a completion-time promise.

The trusted provider must export `open_verification(invocation)` and yield the
exact `VerifyCommerceEvolutionConfig` from
`ExistingEvolutionOperator.open_verification`. The
[existing operator example](../existing-evolution-operator.py) supplies this
binding. Explicitly supply its named `ASHLAR_*` installation, six-target,
ordinary source/ACK and cached-JAR inputs; select the operation capacity actually
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
"$ASHLAR_PYTHON" -m ashlar verify-commerce-evolution-publication \
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
