# Original-pack typed Delta materialization

An installed candidate wheel materialized original UMF 0.8 archaeology and ecology
private graph exports into four fresh local Delta tables. Spark 4.0.1, Delta 4.0.0,
Python 3.12.15 and Java 21 ran sequentially with local[1], 512 MiB driver memory,
UTC and ANSI enabled. The operator completed successfully in 11.298 seconds;
8.260 seconds elapsed after Spark startup. These are functional observations,
not throughput or percentile measurements. No cloud compute was invoked.

Archaeology contains 39 nodes (242 columns) and 46 edges (23 columns); ecology
contains 40 nodes (183 columns) and 54 edges (23 columns). The materializer
recomputed the original projection while holding actual export-file custody,
wrote explicit STRING/BIGINT schemas with errorifexists, read every row at
version 0 with a bounded extra-row sentinel, and compared exact complete bags.
It checked ordered native column types, signed integer values, nulls and
multiplicity, then rechecked both table UUIDs, locations and versions before
closing source/input owners. The receipt is written only after Spark stops.

The original release/custody pairs come from the previously retained
original17-graphframes-native-20261009/original evidence. Exact selected export
hashes and native identities are in result.json. manifest.json binds the candidate
wheel, implementation, tests and archived operator files. Temporary paths in the
operator are historical run inputs, not a supported installation interface.

Six focused materializer/preparation tests passed with
`PYTHONPATH=src:tests:tools python3 -m unittest test_fabric_pack_delta test_fabric_pack_preparation`.
They exercise typed bag preservation, corrupt readback refusal, partial-write
preservation and overlapping destination refusal. Independent scoped code review
approved the implementation before this native check.

This is private-export typed Delta projection/readback evidence. Fabric OneLake
loading, original-pack GQL execution, managed Unity Catalog publication authority
and durable source ACK are separate outstanding qualifications. No storage ACL or
immutability guarantee is inferred from a successful local write.
