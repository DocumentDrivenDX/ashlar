---
title: "Start with the schema package"
description: "Inspect the proposed Delta layout and its synthetic evidence before selecting a deployment target."
eyebrow: "02 / Start here"
composition: model-primary
nextPath: schema/
nextLabel: "See how the tables fit together"
---

## Inspect the candidate

The repository contains the [ashlar-delta/0.3 SQL package](https://github.com/DocumentDrivenDX/ashlar/tree/main/sql/ashlar-delta-v03), a reviewable extraction of the table contract. It is a candidate package with explicit target placeholders, not an automatic installer.

1. Read the package README and enforcement matrix.
2. Inspect `01-baseline.sql` for the six baseline roles.
3. Inspect `02-forward-adjacency.sql` when forward traversal is needed.
4. Select reverse adjacency, degree summaries and typed examples only for an identified consumer workload.
5. Review the publication and consumer contracts before executing reads.

A future deployment must explicitly select a fresh Unity Catalog catalog/schema, managed storage and access policy, then replace the reviewed target placeholders. Existing data requires a separate migration design.

## Understand what the DDL enforces

The candidate declares required-column nullability. It does not enforce typed key uniqueness, endpoint existence, parallel-edge rules or caller authorization. The publisher and execution adapter have separate validation and policy responsibilities.

The [package README](https://github.com/DocumentDrivenDX/ashlar/blob/main/sql/ashlar-delta-v03/README.md) assigns each responsibility. Start there before assuming a declared graph constraint is a warehouse-enforced constraint.

## Inspect the resolver candidate

The Python 3.9+ standard-library resolver requires a trusted table/UUID inventory, supported profile and revision sets, and injected backend/policy ports. It refuses ambiguous descriptors, duplicate JSON members, unsupported revisions and identity mismatches. It keeps exact metadata and returns immutable pins.

Its local checks can be run from a cloned repository:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

These tests use simulated backends. They do not establish live authentication, retention, concurrent publication or a production deployment. See the [resolver README](https://github.com/DocumentDrivenDX/ashlar/blob/main/src/ashlar/README.md) for the integration boundary.

## Follow a synthetic example

Use TypeA A1, TypeB B1 and an independent E1 relationship as a reading example. Find node identity in `object_current`, edge identity/endpoints in `edge_current`, and the original delivery in `source_record`. The journal retains accepted property events; the manifest selects the versions a reader may consume.

Then [inspect ecosystem examples](../ecosystem/) to see how the same canonical meaning becomes an engine-specific release.
