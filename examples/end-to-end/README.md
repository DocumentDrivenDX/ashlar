# End-to-end example — under construction

The first runnable component is the isolated PostgreSQL substrate:

```sh
python3 tools/start_truss_sandbox.py
docker exec ashlar-e2e-truss-pg17 pg_isready -U postgres -d truss_e2e
```

Requires Docker and the PostgreSQL17.9 image; the script creates a labelled
container bounded to512MiB/1CPU and binds only127.0.0.1:15432. Re-running it reuses
that container. It neither installs Truss nor advertises a working data feed.
The generated database password stays in the private container configuration;
it is not printed or committed. Keep the sandbox private.

The [implementation plan](../../docs/helix/04-build/end-to-end-plan.md) tracks the
remaining UMF/schema, Truss runtime/feed, Ashlar publication and additional-source
work. This example will gain actual user-facing commands as those slices run.
Do not use the candidate packages as a production deployment.

## Shared UMF intake

Using a clean source checkout with its dependencies installed:

```sh
bun tools/inspect_umf.ts /path/to/umf EXACT_GIT_REVISION examples/end-to-end/schema-v1.umf.json
python3 tools/check_schema_intake.py /path/to/umf EXACT_GIT_REVISION
```

The checked artifacts use UMF source16c35e8d943769ccfa7bb57d16785aa7159abe65
and Bun1.4.2. This matches the Truss source handoff's UMF baseline; a separate
newer worktree was inspected but missing dependencies, and was not used for the
successful run. schema-v1 is the exact original Truss0.11 source-review model;
schema-v2 adds an optional string field, and schema-unknown retains an unknown
root assertion. The `.intake.json` files preserve original source bytes/digests
and UMF diagnostics. All examples are structurally valid, but UMF's experimental
warnings keep completeInterpretation false. Do not equate that flag with target
catalog acceptance or silently remove warnings to make it true.

[CONTRACT-005](../../docs/helix/02-design/contracts/CONTRACT-005-schema-intake.md)
owns this shared intake boundary. Native Truss/Ashlar registry persistence,
semantic binding, stable catalog allocation and schema evolution are next;
this inspection component does not yet claim to feed a running Truss engine.
