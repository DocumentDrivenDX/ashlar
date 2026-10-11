# Fresh private original-commerce source and ACK fixture

This explicit development operation uses the existing `tools/setup_private_outbox_fixture.py` owner. It does not initialize Delta, append commerce events or publish. The example creates a NEW local PG17.9 ARM64 fixture with CPU1/memory512MiB; it must never be applied as a repair to retained C/E/F fixtures. Creation requires separately reviewed operator authorization and current name/port/output absence checks.

Copy and independently select `configuration.example.json`. Every absolute Docker executable/socket/output, name, port and scope is an explicit selection; there is no default Docker context or credential discovery. The example socket/executable are one local workstation choice, not production configuration. Select an existing cached exact image digest; the tool refuses pull or a different image/architecture. Port25435 and the container name are proposed inputs, not reservations. Use a new output whose canonical parent exists.

Run from an environment with the installed/checkout Ashlar owners and explicitly selected psycopg SDK, using the setup tool's public command:

```sh
python -B tools/setup_private_outbox_fixture.py --configuration /absolute/selected-fixture.json
```

The actual `inventory.json` must be retained before constructing publisher configuration. It records actual positive container identity, returned source signatures, current source/ACK bindings and isolated trust qualification. Never fabricate inventory/signatures. Source A has exact feed `private-original-commerce-fixture` and epoch `original-commerce-v1`, separate LOGIN NOINHERIT writer/reader/ACK roles and a protected consumer. Use its `scope.installation_id` as the explicit installation identity in the fresh native initializer and require the publication handoff to match. Source B is a distinct unused scope required by the existing two-source setup tool; it remains empty and grants no original-commerce authority.

The fixture admin phase installs the original source/service DDL and narrow roles, then closes before ordinary authority checks. Only the source writer can append; the reader is read-only and the ACK role has protected service execution without source append/write privileges. No broader grants, native-schema SQL, Truss producer or production authentication are introduced.

After successful fixture preparation, follow the separately documented installed initializer and publisher in [the original-commerce guide](../original-commerce-local/README.md). They append the exact original21events only through the public original source/publisher owners. This fixture alone proves neither source content nor 11-object/10-edge bags, native publication, durable query results, or general recovery. Retain the positively owned container; cleanup must select its actual ID and cannot delete an unrelated matching name.
