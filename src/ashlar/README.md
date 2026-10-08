# Publication resolver candidate

Python3.9+ standard-library core implementing the proposed
[CONTRACT-004](../../docs/helix/02-design/contracts/CONTRACT-004-publication-resolver.md).
No credentials, cloud SDK, writes or automatic deployment are included.

resolve_publication requires an explicit trusted table/UUID inventory, supported
profiles/revisions and a backend. It refuses missing/ambiguous descriptors,
duplicate JSON members, malformed pins, unsupported revisions and native identity
mismatches, and returns defensive immutable metadata. Unknown original JSON text,
large integers and exact decimals are retained. Snapshot versions never follow
latest physical heads implicitly.

NativeBackend builds read-only parameterized descriptor SQL and pinned metadata
probes through an injected authenticated Executor. The Policy must independently
validate descriptor authority, effective caller policy and retained data availability.
There is no permissive default policy. This is the first runtime slice, not a
complete native transport, publisher, authorization service or support claim.
The existing SQL package supplies table definitions and singleton templates.

Run the small local suite from repository root:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The suite covers refusal order, exact metadata, immutable pins, native UUID/version
mismatches and pinned schema checks. No live warehouse or Spark is used. Next wire
an authenticated transport and qualified policy/custody provider into these ports;
keep their integration claims separate from these simulated checks.

SchemaIntake.read adds exact-byte shared UMF custody under CONTRACT-005. Supply
an explicit document revision and trusted validator source pin. It retains the
original source and diagnostic artifact, checks digests/identity/validation flag
consistency and refuses malformed input. It does not authenticate the producer
or perform target catalog acceptance; trust and admission are separate.

plan_catalog_ids in catalog.py plans exact document/owner-qualified IDs from a
complete admitted target inventory and trusted prior state. It preserves retired
reservations and same-identity reactivation; allocation stays above supplied
highwaters. Native acceptance must serialize/revalidate/persist the plan with
its full semantic and report effects. Pure planning does not provide acceptance,
authorization, field binding or a native allocator.
