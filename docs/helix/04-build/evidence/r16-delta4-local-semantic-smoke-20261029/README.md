---
ddx:
  id: ashlar.evidence.r16-delta4-local-semantic-smoke-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: CONTRACT-003
      kind: governed_by
    - id: ashlar.evidence.truss050-r16-recovery-packet-candidate-20261029
      kind: informed_by
---

# R16 local Delta 4.4.1 table semantic smoke

**Result: bounded local table smoke passed.** Ashlar's pinned UMF-backed six-role
table creator created fresh Delta 4 tables with profile
`ashlar-delta/0.4-xxhash64`. Its native schema, protocol, clustering and layout
checks completed for all six roles. Two tiny rows written with native Spark SQL
round-tripped their signed 64-bit typed-key hashes through the object and edge
tables. This is a runtime/table seam check, not a release pass.

The run used the isolated R16 cohort at
`/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029`: Python 3.12.14,
PySpark 4.2.0, Delta Spark 4.4.1, Java 21.0.13.0.2, and `local[1]`. The Delta
Spark and Delta storage JAR hashes were respectively
`2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be` and
`f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f`. The
Spark JAR was copied byte-identically from the cohort's
`delta-spark_2.13-4.4.1.jar` to the legacy filename required by Ashlar's current
runtime guard; the storage JAR retained its original filename. Both copied
hashes appear in [result.json](result.json).

| Role | Rows | Native Delta UUID |
| --- | ---: | --- |
| object_current | 1 | `bb6a704c-2889-42ec-af67-0213d95f12f0` |
| edge_current | 1 | `43543e6e-ef35-4f60-94aa-5529d7442b28` |
| tombstone | 0 | `00f50087-1a35-4ca8-a5a3-ef79b60d5339` |
| whole_source_history | 0 | `52a1dc8c-aeef-49a5-91b3-00873ad960d0` |
| attempts | 0 | `c580f12f-a16c-4ca5-b381-320d67c35a1f` |
| manifest | 0 | `7d1802e8-6671-47f9-97bb-ebcbbd296004` |

The object key `(smoke-source, type_id=11, id=22)` stored hash
`1437292216916987980`; the edge key `(smoke-source, rel_type_id=33, id=44)`
stored `8656680201443444023`. Each readback recomputed Spark `xxhash64` from the
STRING source and BIGINT type and ID columns and matched the stored BIGINT value.
The six-role creator also compared each native field list, protocol, clustering
columns and authored layout properties to the pinned UMF proposal before
returning.

The `.4` configured-local lifecycle adapter currently permits initialization
only. Therefore the two test rows were direct Spark Delta SQL inserts into this
fresh private installation; they were not an Ashlar publication. No Truss feed,
publication manifest, singleton resolver, Weft query, source mutation, protected
ACK, Unity Catalog, production authority, export, or scale claim resulted.

The exact script is [smoke.py](smoke.py), SHA-256
`9b9b83248de260edca5335c454e6c0c1ce5dd4973ccaa08e8ef1f2abd72fbb4c`. Run it
from the repository root with:

```sh
PYTHONPATH="$PWD/src" \
JAVA_HOME=/opt/homebrew/Cellar/openjdk@21/21.0.13.0.2/libexec/openjdk.jdk/Contents/Home \
PYSPARK_PYTHON=/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python \
PYSPARK_DRIVER_PYTHON=/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python \
SPARK_LOCAL_IP=127.0.0.1 PYTHONDONTWRITEBYTECODE=1 \
/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python \
docs/helix/04-build/evidence/r16-delta4-local-semantic-smoke-20261029/smoke.py
```

The successful run took 19.291 seconds and its owned output measured 9,576 KiB.
The three direct-native attempt directories together measured 28,400 KiB. One
pre-Spark attempt through the configured-local lifecycle refused because the
retained UMF producer path in the old packet was not a usable Git repository.
A direct Spark setup attempt then stopped before table creation on an
unnecessary `CREATE DATABASE local.configured` statement; removing that setup
statement allowed the pinned Ashlar table creator to run. Both attempts remain
in private temporary storage and created no Delta role tables.

During that producer-path investigation, a temporary shared Git clone grew to
4,661,640 KiB, exceeding the requested 2 GiB additional-disk bound. Removal
completed, and `/private/tmp/ashlar-r16-delta4-smoke-20261029/umf-source/producer`
is absent. The remaining investigation directory is 9,436 KiB; the separate
successful smoke output directory is 28,400 KiB. The clone was the only
oversized temporary artifact; it was not used for the passing smoke and did not
start Spark. This bound breach must remain visible in the review record.
