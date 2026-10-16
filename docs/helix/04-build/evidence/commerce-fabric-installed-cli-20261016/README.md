# Installed commerce-to-Fabric Delta command

The installed `python -m ashlar materialize-commerce-fabric` command completed
against the existing original commerce publication and ordinary PostgreSQL
source/ACK owner. It wrote 11 nodes and 10 independent edges into fresh local
Delta tables, checked complete ordered STRING schemas and full row bags, closed
its owners, then released the complete 99,531-byte report with mode `0600`.
The supervised process exited zero in 58.851 seconds under a 240-second deadline.
This is one small functional run, not a throughput benchmark.

The output release hash, manifest and complete snapshot vector exactly match
[the preceding held API materialization](../commerce-fabric-delta-native-20261016/README.md).
All 78 original native file hashes remained identical. The new node UUID is
`f633a7ad-d4de-4d99-98bb-5fea7411b0c4`; the new edge UUID is
`eca34bbe-b3ea-40b2-95f0-bc47c3ccc9ff`, both version zero. The original properties
remain exact opaque JSON STRING carriers. The report does not grant immutable
projection access or Fabric loaded-generation authority.

A fresh wheel from main `6d67698130d17d508450096e9bc94462c1843d87` was installed
into a new copied Python virtual environment. All 409 installed Ashlar files are
hash-pinned and correspond to the wheel. The run used Python 3.12.14,
SQLite 3.53.1, selected Spark 4.0.1/Delta 4.0.0 and JDK 21, with local[1] and
a 512 MiB driver. Existing SDK links were reused; this is not a fresh whole-SDK
or OS closure. The command independently checks its selected SDK versions and
the exact two Delta JAR hashes before Spark starts.

The initial command attempt refused after 9.230 seconds, with no table or report
created. A separately retained diagnostic invocation exposed a connection refusal
at the configured loopback PostgreSQL port. The exact retained container
`79d2e5975ec785446b2f2036d92ab41b837a6363e4f6737014fe0f559456963b` was stopped.
It was restarted with its existing database, one CPU, 512 MiB memory and
127.0.0.1:25439 mapping; readiness was verified before the successful attempt.
No source fixture, batch, publication, grant or ACK was created. All native
attempts retain separate logs/status and unchanged original file inventories.

An earlier preparation attempt used obsolete system setuptools 58 and produced
an empty UNKNOWN wheel; its zero installed Ashlar files were detected before any
native invocation. That preparation is retained as a failure. The corrected
build used the available setuptools 84/wheel 0.48 backend, a minimal tracked
source archive, offline installation and an installed-file count check. Passing
runtime evidence belongs only to the corrected installation and second command
attempt; neither earlier failure is counted as a pass.

manifest.json pins 38 exact compressed records, uncompressed lengths and hashes:
build/install inventories, executed preparation/supervision, both command attempts,
the diagnostic refusal, restored source state, selected runtime metadata, full
report, unchanged original files, native Parquet and Delta commits. Independent
review checks installed byte correspondence, actual results and limitations.
No cloud compute, upload, graph load or ACK submission occurred. The separate
[Fabric cloud proof](../commerce-fabric-publication-gql-20261016/README.md) qualifies
the earlier projection's OneLake/load/GQL correspondence, not these new targets.
