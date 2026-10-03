---
ddx:
  id: FEAT-001
  type: feature-specification
  activity: frame
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
---

# Feature Specification: FEAT-001 — UMF graph profile

**Feature ID**: FEAT-001
**Status**: Draft
**Priority**: P0
**Owner**: Ashlar owner; technical lead to designate
**Covered PRD Subsystem(s)**: UMF graph profile
**Covered PRD Requirements**: FR-1
**Cross-Subsystem Rationale**: None — single subsystem.

## Overview

Engineers can interpret a graph model without changing its source semantics.
DocumentDrivenDX's [UMF](../../00-discover/resources/umf.md) is the metamodel
input, not a graph execution engine. A node type describes instances; a node
is an instance. A relationship type describes permissible links; an edge is a
property-bearing instance with its own identity.

## Ideal Future State

An engineer can explain every selected node/edge construct, its warehouse
mapping, and any meaning retained only in the source or loss report.

## Problem Statement

The brief's ontology vocabulary is not yet bound to a specific UMF revision.
Assuming entity/property words are aliases for existing UMF concepts could
silently change identity, cardinality, or relationship meaning.

## Functional Areas

| Area | Responsibility |
| --- | --- |
| Logical profile | Define the supported graph subset and UMF mappings |
| Projection accountability | Account for retained, transformed, or unsupported meaning |

## Requirements

### Logical profile

- **MODEL-01:** The system must accept consumer-defined types across domains,
  without built-in industry schemas, and identify an exact UMF core and extension subset
  for types, properties, keys, nullability, cardinality, and relationships.
- **MODEL-02:** The system must preserve node/edge identity separately from
  display names and physical names, including isolated nodes and parallel edges.
- **MODEL-03:** The profile must define direction, endpoint types, property
  types, and supported multiplicities; unsupported self-links, polymorphism,
  or multi-label semantics must be explicit rather than inferred.
- **MODEL-04:** Graph-only semantics must use a versioned Ashlar extension where
  existing UMF vocabularies cannot express them. Graph inclusion hints must
  reference the same logical model, not duplicate its type declarations.

### Projection accountability

- **MODEL-05:** The system must retain original metadata and classify each
  selected mapping as preserved, approximated, unsupported, or unknown.
  Recovery using retained source must be distinguished from target-only recovery.
- **MODEL-06:** The profile must account for value/edge provenance and version,
  optional interoperability identifiers, and governance declarations. Public,
  private, group, and high-visibility terms from the brief remain unresolved
  policy vocabulary until defined; a tag must not imply runtime enforcement.

### Non-Functional Requirements

- Zero unexplained semantic losses across the selected corpus.
- 100% of unknown graph-affecting constructs reported before projection acceptance.
- Synthetic fixtures only; zero production or personal data in this milestone.

## User Stories

- [US-001 — Review a graph mapping](../user-stories/US-001-review-graph-mapping.md)

## Edge Cases and Error Handling

Ambiguous source identity, unresolved endpoint types, unknown extensions, and
stale logical bindings must prevent a supported-mapping claim. Retain the source
and report the offending construct. A renamed display label must not silently
create a new logical identity.

## Success Metrics

An engineer can account for 100% of authored constructs in the small domain-neutral
fixture, including one deliberately unsupported rule, without undocumented aliases.

## Constraints and Assumptions

No UMF core changes are authorized. Existing physical binding must be assessed
before extending it. Exact schema vocabulary belongs in a subsequent Contract;
PRD Q1 and Q2 block finalizing that surface.

## Dependencies

PRD FR-1; pinned UMF core and binding contracts. No Truss or Axon runtime dependency.

## Out of Scope

Actions, compensation, executable domain semantics, UMF cleanup, RDF runtime,
and ontology authoring UI.

## Review Checklist

- [x] One PRD subsystem, testable behavior, named story, and explicit boundaries.
- [x] Exact shared surfaces deferred to Contracts, not invented here.
- [ ] Design decisions, executable acceptance evidence, and owner approval.
