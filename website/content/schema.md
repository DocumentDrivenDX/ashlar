---
title: The physical schema
description: Six baseline roles preserve current state, source evidence and publication visibility. Optional projections serve selected workloads.
eyebrow: 04 / Schema
composition: model-primary
nextPath: background/
nextLabel: Read why the structure looks this way
---

## The table map

On a narrow screen, scroll the diagram horizontally to read every field.

This is a logical map of the proposed **ashlar-delta/0.3** package. Arrows show data derivation or validated references, not warehouse-enforced foreign keys. The manifest pins every role required by a selected publication.

<figure class="schema-map">
<svg viewBox="0 0 800 710" role="img" aria-labelledby="schema-title schema-desc">
<title id="schema-title">Ashlar Delta table roles and relationships</title>
<desc id="schema-desc">Source records retain deliveries. Current objects and edges retain typed identities and exact carriers. Edges reference objects through typed endpoints. Property journal and tombstone retain accepted history and deletion evidence. Optional adjacency and typed serving projections derive from canonical state. Publication manifest pins required table identities and versions after validation.</desc>
<defs><marker id="schema-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0 0L10 5L0 10Z" fill="#234fbc"/></marker></defs>
<g fill="none" stroke="#234fbc" stroke-width="2" marker-end="url(#schema-arrow)">
<path d="M400 105V140H170V175"/><path d="M400 140H610V175"/>
<path d="M505 229H300" stroke-dasharray="6 5"/>
<path d="M90 280V360"/><path d="M245 280V326H400V360"/>
<path d="M625 280V360"/><path d="M680 280V540H645V575"/>
<path d="M170 470V540H365V575"/><path d="M400 470V575"/>
</g>
<g fill="#f5f3ec" stroke="#234fbc" stroke-width="1.5">
<rect x="270" y="20" width="260" height="85"/>
<rect x="40" y="175" width="260" height="105"/>
<rect x="505" y="175" width="260" height="105"/>
<rect x="20" y="360" width="260" height="110"/>
<rect x="300" y="360" width="200" height="110"/>
<rect x="530" y="360" width="250" height="110" stroke-dasharray="6 5"/>
<rect x="20" y="575" width="250" height="105" stroke-dasharray="6 5"/>
<rect x="305" y="575" width="460" height="105"/>
</g>
<g fill="#202b2d" font-family="monospace" font-size="17" font-weight="bold">
<text x="290" y="48">source_record</text><text x="60" y="203">object_current</text><text x="525" y="203">edge_current</text>
<text x="40" y="389">property_journal</text><text x="320" y="389">tombstone</text><text x="550" y="389">adjacency</text>
<text x="40" y="604">typed projections</text><text x="325" y="604">publication_manifest</text>
</g>
<g fill="#526267" font-family="monospace" font-size="13">
<text x="290" y="73">feed / epoch / delivery_id</text><text x="290" y="92">payload + cursor + digest</text>
<text x="60" y="228">source / type / id</text><text x="60" y="249">props_json + retained_json</text><text x="60" y="269">version + source reference</text>
<text x="525" y="228">source / rel_type / id</text><text x="525" y="249">typed endpoints + bags</text><text x="525" y="269">version + source reference</text>
<text x="327" y="217">typed endpoints</text>
<text x="40" y="415">old/new presence + tokens</text><text x="40" y="437">origin + event ordinal</text><text x="40" y="459">accepted property events</text>
<text x="320" y="415">typed deletion key</text><text x="320" y="437">entity_version</text><text x="320" y="459">source reference</text>
<text x="550" y="415">edge_id + endpoints</text><text x="550" y="437">structural_version</text><text x="550" y="459">optional forward/reverse</text>
<text x="40" y="630">selected scalar columns</text><text x="40" y="652">explicit coverage/residuals</text>
<text x="325" y="630">table UUIDs + versions / progress / revisions</text><text x="325" y="652">validated immutable publication boundary</text>
<text x="22" y="523">Manifest pins required roles, including selected projections.</text>
</g>
</svg>
<figcaption>Solid boxes: baseline roles. Dashed boxes: workload-selected projections. Edge endpoint references are validated by the publisher.</figcaption>
</figure>

## Canonical current state

| Table | Semantic key | Contents |
| --- | --- | --- |
| `object_current` | source system + type ID + object ID | Exact property/retained text, logical key, revision, entity version and source origin |
| `edge_current` | source system + relationship type ID + edge ID | Independent edge identity, both typed endpoints, exact bags, revision/version and source origin |

Both have a derived `lookup_hash` for physical lookup. Read predicates retain the full native tuple. The hash does not enforce uniqueness or replace native identity.

## Evidence and lifecycle roles

| Table | Role | Boundary |
| --- | --- | --- |
| `source_record` | Original delivery envelope, cursor and digest | Original bytes are additional to parsed current state |
| `property_journal` | Old/new property presence and exact tokens, origin and event ordinal | Accepted source history; independent of Delta physical change history |
| `tombstone` | Typed deletion identity, entity version and source origin | Source-profile ordering must prevent stale resurrection |
| `publication_manifest` | Required table identities/versions, source progress, revisions and validation report | Readers select the completed vector, never an implicit latest-head mix |

## Optional serving roles

`adjacency_forward` clusters structural rows around the source endpoint; `adjacency_reverse` serves reverse access when selected. `degree_summary` supplies declared directional counts. Typed node and edge examples expose selected scalar properties for graph tools.

Every projection keeps independent edge identity where relevant, typed endpoint closure and explicit coverage. Its exact release version joins the publication boundary. These roles are rebuildable and do not replace retained canonical content.

## Baseline DDL excerpt

This excerpt shows identity and carrier columns from the packaged object definition. It is deliberately incomplete; use the linked full SQL for all required columns and physical settings.

```sql
CREATE TABLE object_current (
  source_system STRING NOT NULL,
  type_id BIGINT NOT NULL,
  id BIGINT NOT NULL,
  logical_key_json STRING NOT NULL,
  schema_revision STRING NOT NULL,
  entity_version BIGINT NOT NULL,
  props_json STRING NOT NULL,
  retained_json STRING NOT NULL,
  -- Additional source, publication and lookup columns follow.
  ...
);
```

[Complete baseline SQL](https://github.com/DocumentDrivenDX/ashlar/blob/main/sql/ashlar-delta-v03/01-baseline.sql) · [Forward adjacency SQL](https://github.com/DocumentDrivenDX/ashlar/blob/main/sql/ashlar-delta-v03/02-forward-adjacency.sql)

## Declared and enforced are separate

Delta nullability declarations are present. Typed uniqueness, endpoint existence, projection completeness, replay policy, authorization and multi-role publication require external validation/enforcement. The [enforcement matrix](https://github.com/DocumentDrivenDX/ashlar/blob/main/sql/ashlar-delta-v03/README.md#enforcement-responsibilities) assigns each responsibility.
