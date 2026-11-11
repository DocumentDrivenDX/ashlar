# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- Removed committed run output, receipts and qualification packets, process-only tests and
  the one-off release workflows. CI is `ci.yml` (lint, tests on Python 3.11 and 3.13, wheel
  build with quickstart) and a nightly slow/Spark job.
- Removed the Databricks live-trial runners and checkers. The managed notebooks moved to
  `examples/databricks` and are labelled experimental.
- Removed the Truss 0.0.49-0.0.52 feed and port adapters that no live code imported.
- The microsite builds the UMF schema browser from npm and shows the released version in
  its footer. Innsigle page signing was dropped.
- README quickstart is run by CI (`scripts/quickstart.sh`).
- Added a tag-driven `release.yml` (build, fresh-venv smoke test, PyPI trusted publishing,
  GitHub Release).

### Fixed
- `configured_local_runtime._require` accepted no message although its callers pass one.
- Repaired test fixtures that had drifted from the code (delta4 metadata operation,
  existing-driver, configured-local runtime, read-only transport, A1 trial plan).

## [0.1.0] - 2026-11-03

First release, published as a GitHub release (not on PyPI).

- Delta property-graph tables, publication boundary, consumer read boundary and resolver contracts.
- Local Truss to Delta ingest with source and acknowledgement custody, and publication-bound Weft reads.
- Graph export for GraphFrames and PuppyGraph, with parallel edges and isolated nodes preserved.
- A Databricks serverless (Spark 4.2.0, Unity Catalog) publication profile, exercised once by hand
  and not by CI.
- Local cohort: Truss 0.0.55, UMF SDK 0.10.2, Weft 0.4.56.
