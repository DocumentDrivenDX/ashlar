---
ddx:
  id: ashlar.product-vision
  type: product-vision
  activity: discover
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.resource.graphframes
    kind: informed_by
  - id: ashlar.resource.graphar
    kind: informed_by
  - id: ashlar.resource.databricks-constraints
    kind: informed_by
---

# Product Vision

## Mission Statement

Ashlar is an open-source toolkit that helps data platform engineers define and
use property-graph ontologies on data warehouses through UMF, trustworthy
table schemas, and explainable queries.

## Positioning

For data platform teams building ontologies across domains,
Ashlar is an open-source toolkit for defining and using ontologies on a data warehouse. It replaces repeated
handwritten structural mappings with a reusable metadata contract. The owner
selected Databricks and integration with DocumentDrivenDX's UMF, a metamodel
and schema interchange fabric. The first user team remains to be identified.

## Vision

Engineers describe typed entities and relationships once, preserve their meaning
across warehouse representations, and query connections with known limits.
Both nodes and edges carry properties; source lineage remains inspectable.

**North Star**: Two independent producers and one analytics consumer share a
versioned ontology without producer-specific structural remapping in the consumer.

## User Experience

A platform engineer describes synthetic TypeA, TypeB, and TypeC node types
and their property-bearing relationships. The engineer reviews the warehouse representation,
sees which constraints require external checks, and queries nodes
reachable through a fixed two-hop pattern. An isolated node remains discoverable. A missing
endpoint or unsupported mapping is visible before the data is trusted.

## Target Market

| Attribute | Description |
| --- | --- |
| Who | Data platform engineers and ontology analytics engineers across domains; Databricks is the first target. |
| Pain | Identity, relationship, and property conventions diverge across source systems. |
| Current Solution | Handwritten table mappings and query-specific joins; prevalence is an unvalidated hypothesis. |
| Why They Switch | Reuse a reviewed ontology contract and see its preservation and enforcement limits. |

## Key Value Propositions

| Value Proposition | Customer Benefit |
| --- | --- |
| Shared logical definitions | Teams reuse the same entity and relationship meaning. |
| Explicit projection limits | Consumers distinguish retained meaning from approximations and omissions. |
| Queryable warehouse ontology | Analytics follows connections using a supported, evidenced path. |

## Success Definition

Proposed strategic measures for the first 12 months after a pilot starts:

| Metric | Target |
| --- | --- |
| Adoption | Two independent producers and one consumer pass a shared conformance corpus. |
| Reuse | Zero producer-specific structural remapping in the pilot consumer, assessed by integration review. |
| Preservation | Zero unexplained semantic losses in the agreed mapping corpus. |

## Why Now

The owner's 2026-10-03 direction establishes Ashlar as a general-purpose,
open-source property-graph toolkit and prioritizes warehouse schemas and queries.
Framing this boundary now prevents transactional-engine and agent-interface
work from obscuring the foundational schema decisions. Customer validation and
billion-node feasibility remain open.

## Review Checklist

- [x] Direction, audience, concrete scenario, and measurable outcomes are stated.
- [x] Detailed requirements and physical choices are delegated downstream.
- [ ] Owner approval and first-user validation remain pending.
