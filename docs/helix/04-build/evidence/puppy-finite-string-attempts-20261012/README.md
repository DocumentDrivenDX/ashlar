# Two authored finite STRING query attempts

The first actual attempt selected the unchanged archaeology export and current public finite-source/file owners. It exited 1 before Docker launch: DuckDB 1.4.1 treated `carrier.nodes` as ambiguous because both the database file and schema were named `carrier`. The preparation schema was renamed consistently to `finite_projection`; the release/custody bytes and authored queries were unchanged.

A separate real-owner DuckDB-only check then exited 0, comparing all 39 archaeology nodes and 46 edges with the independent original projection and closing its owners. No engine was opened by that check.

The next fresh-path attempt used the same cached PuppyGraph image and unchanged limits: 2 CPUs, 3 GiB, 120 seconds per sequential case. It exited 1 during readiness when the HTTP connection closed without a response. No model or authored query was submitted. Owned cleanup ran; a read-only Docker listing restricted to the exact test-name prefix found no remaining container. The original attempts and generated partial files remain local. No successful native query, ecology execution, or broader engine qualification is claimed.

The following fresh-path C attempt passed readiness but exited 1 when the single
model-submission POST timed out while waiting for its HTTP response. No successful
model response or native query was retained. Submission outcome is **unknown**;
the timeout does not prove that the model was not applied. The owned container
was removed, and the operator's read-only Docker listing of the exact owned prefix
found no remaining container. No POST retry or native rerun followed in this turn.
The source correction gives that one model-submission socket at most 30 seconds,
limited by the same remaining 120-second interval; GET/startup and query limits,
2-CPU/3-GiB resources and the original exports remain unchanged.

The D attempt used fresh paths and a single model-submission socket allowance of
30 seconds within the same 120-second interval. Readiness completed, but the POST
again timed out before a response. Its submission outcome is unknown; no query
ran and no model-success receipt was returned. A read-only Docker listing
restricted to the owned test prefix was empty after terminal cleanup. Native retries are stopped pending a request/configuration diagnosis against
the working recipe.
