# Two authored finite STRING query attempts

The first actual attempt selected the unchanged archaeology export and current public finite-source/file owners. It exited 1 before Docker launch: DuckDB 1.4.1 treated `carrier.nodes` as ambiguous because both the database file and schema were named `carrier`. The preparation schema was renamed consistently to `finite_projection`; the release/custody bytes and authored queries were unchanged.

A separate real-owner DuckDB-only check then exited 0, comparing all 39 archaeology nodes and 46 edges with the independent original projection and closing its owners. No engine was opened by that check.

The next fresh-path attempt used the same cached PuppyGraph image and unchanged limits: 2 CPUs, 3 GiB, 120 seconds per sequential case. It exited 1 during readiness when the HTTP connection closed without a response. No model or authored query was submitted. Owned cleanup ran; a read-only Docker listing restricted to the exact test-name prefix found no remaining container. The original attempts and generated partial files remain local. No successful native query, ecology execution, or broader engine qualification is claimed.
