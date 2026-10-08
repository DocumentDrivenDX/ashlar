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
