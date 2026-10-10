---
ddx:
  id: ashlar.puppygraph-operator
  type: runbook
  activity: deploy
  status: draft
  authoring:
    home: repo
  links:
  - id: CONTRACT-003
    kind: informed_by
  - id: ADR-001
    kind: informed_by
---

# PuppyGraph release activation and reads

Use an exact original graph release and an explicitly selected operator provider.
The provider is trusted Python code, selected by its SHA256; it supplies ordinary
native transports, prepared carrier receipts, current source/native policy, and
resource cleanup. It exposes `open_puppy_release(invocation, release)` as a context
manager yielding `ashlar_host.puppy_release_cli.PuppySession`. Its adapter must
be an exact `PuppyNativeReleaseAdapter` instance. Its policy implements
`writer(context)` and `admit(handle, context)`, independently admitting the
current source and native authority; successful admission returns `None`.

Use `ashlar_host.puppy_native.PuppyNativeReleaseAdapter` with a supplied Cypher
transport, or pass `ashlar_host.puppy_gremlin.GremlinReleaseProjection` as that
adapter’s `projection=` argument with an ordinary Gremlin transport. The
projection supplies query observations through the same adapter. Both pass through the same immutable release owner, complete
value and endpoint oracle, schema/carrier checks, and closing admission gates.
Provider hashes establish byte custody; current policy independently authorizes
source use and native operations.

Prepare the immutable local DuckDB carrier with `ashlar prepare-puppy-release`
before activation. This command accepts `--configuration`, `--provider` and
`--provider-sha256`. The closed configuration is:

```json
{
  "profile": "ashlar-puppy-preparation-invocation/0.1",
  "release_path": "/private/ashlar/original.graph.json",
  "release_sha256": "<externally trusted original release SHA256>",
  "output_directory": "/private/ashlar/new-carrier"
}
```

The preparation provider exposes `open_puppy_preparation(invocation, release)`
as a context manager yielding `PuppyPreparationSession(policy, context)` from
`ashlar_host.puppy_carrier_cli`. Its policy implements
`admit_original_release(release, context)` and returns `None` only after admitting
the original publication lineage and current source/read authority. This gate is
renewed before committing the carrier and completing preparation. The command
requires DuckDB in the selected environment; its import follows initial release
admission. The destination must be new, absolute, and under an owned private
parent. The output preserves exact original String/null values, graph identities
and incident endpoints in two VARCHAR tables, plus the hash-qualified native
model. A 31-character catalog name is a shortened storage name; full release and
carrier hashes remain authoritative byte pins, and changed model collisions
refuse during activation.

The command retains `original-intent.json`, the release-qualified `.duckdb`
file, `model.json`, and `receipt.json`. The completed receipt follows provider
cleanup and closing original-file checks. A failed preparation may leave owned
incomplete files; it cannot be resumed by treating their presence or absence as
authority. Prepared artifacts do not activate PuppyGraph, extend source grants,
or qualify native queries. The activation provider must independently admit
current native/source authority and the selected exact carrier again.

An activation configuration has this closed shape. Paths must be absolute;
`output_directory` must be a fresh child of an owned private directory.

```json
{
  "profile": "ashlar-puppy-invocation/0.1",
  "release_path": "/owned/releases/original.graph.json",
  "release_sha256": "<exact original graph SHA256>",
  "output_directory": "/owned/results/activation-r1"
}
```

```sh
ashlar activate-puppy-release --configuration /owned/activate.json \
  --provider /owned/operator.py --provider-sha256 "$OPERATOR_SHA256"
```

The command retains and syncs original intent before executing the provider.
Success returns an `activation_path`, its exact `activation_sha256`, and an
`observation_path`. Retain that original receipt and trust its digest explicitly.
To read in another process, add these two fields to a read configuration and use
a fresh output directory:

```json
{
  "activation_path": "/owned/results/activation-r1/activation.json",
  "activation_sha256": "<externally selected original receipt SHA256>"
}
```

```sh
ashlar read-puppy-release --configuration /owned/read-r1.json \
  --provider /owned/operator.py --provider-sha256 "$OPERATOR_SHA256"
```

The read configuration retains the activation configuration's profile, original
release path and digest. Reading verifies the selected native engine, immutable
release schema, carrier, full values and endpoints under current policy. A
provider may use Gremlin to read a release activated through Cypher.

Each receipt selects one original handle. It does not create a remote latest
alias, grant missing-state repair, or authorize an automatic retry after uncertain
activation. Preserve uncertain intent and reconcile through independently owned
native authority before any further mutation. Engine restart or changed schema,
carrier, source authority, provider bytes, or original files can cause refusal.

Public APIs are installed with Ashlar; a provider must not depend on repository
`tools` modules. Native SDKs and prepared data remain explicit operator inputs.
