# Publish the original commerce source explicitly

Use `ashlar_host.original_commerce_publication.publish_original_commerce` from
an installed Ashlar package. This selected local development workflow preserves
the original 21 source events, model, graph and carrier bindings. Evolution R1
is a separate source profile even when its graph values match.

First select independently established ordinary source writer, reader and
protected ACK sessions. Provisioning is a separate explicit fixture setup;
the publication provider creates no database, roles or grants and discovers no
credentials. Keep the two evolution scopes reserved for their original run.
A separate original-commerce scope must use feed
`private-original-commerce-fixture` and epoch `original-commerce-v1`.

Separately initialize six empty Delta tables (`object_current`, `edge_current`,
`tombstone`, `whole_source_history`, `attempts`, `manifest`) and the original
journal with the public `LocalDeltaTransport.initialize` port under explicitly
admitted empty-installation authority. Retain the actually observed UUIDs and
reservations. Initialization is not performed by the publication operator.

Construct `OriginalCommerceSourcePorts` with the exact
`OutboxAuthorityRegistration`, a separate ordinary writer role and writer
connection factory. Construct `OriginalCommercePublicationOperator` with the
selected installation ID, canonical journal path, six actual `DeltaTarget`s,
source ports, existing operation capacity and an owned native-session context.
Connection factories receive the actual operation context and return fresh
ordinary transactions. Their descriptors do not establish authority.

Pass `OriginalCommercePublicationConfig(output, model, graph, jars, producer)`
to `publish_original_commerce(config, operator=operator)`. All paths are explicit
and absolute. The output must be new. Use Python 3.11, Spark 3.5.3, Delta 3.2.1
and the exact four publication JARs identified by `ashlar_host.commerce.PUBLISH_JARS`.
The selected public UMF producer validates the original model/graph; no SQL is
rewritten. The installation uses the same cooperating-operator lock as evolution.

The workflow admits initial publication and same-invocation replay. If an earlier
source append committed before publication began, a fresh invocation can reuse
that exact head-one transaction while the native installation remains empty.
Any other source bytes, extra source progress, nonempty native state or lost
journal refuses. This is not general publication restart recovery. A report is
released only after the actual protected ACK and all closing cleanup/custody.

For the separately indexed 0.4.2 query profile, supply both an ordinary ACK
connection factory and `CommerceQuerySourceFactory` bound to this original
registration. Preserve the report's explicit journal, installation and role
fields; they do not authorize access. Execute the four unchanged original SQL
cases under full original publication/source holds and independent complete-bag
comparison. The query's selected Spark 4 runtime is separate from the publication
runtime. Qualification requires separately retained actual execution evidence;
this API does not claim Truss acceptance or production authority.
