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
