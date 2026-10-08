---
ddx:
  id: ashlar.design-system
  type: design-system
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
---

# DESIGN.md — Ashlar microsite

Design a developer microsite for engineers evaluating property graphs on
Databricks warehouse tables. Lead with the graph model and its practical value;
offer a concrete schema example before exact reference material.

Catalog: installed HELIX 0.15.0 `workflows/graph.yml`. This instance uses the
design-system template and prompt, with the product-microsite-ia practices.
The [homepage prototype](microsite/index.html) records the initial visual design.
The complete microsite lives in `website/`; [ADR-002](adr/ADR-002-signed-static-microsite.md) governs build and deployment.

## Navigation and Active State

Global navigation: Overview, Concepts, Schema, Background, Ecosystem, Start here, Reference, and Source.
Overview is the homepage. Concepts explains typed nodes, property-bearing edges,
identity, and publication snapshots. Start here leads to the schema package
and synthetic examples. Reference leads to exact contracts and evidence.
Source links to the repository. Each primary destination is visible directly.

| Reader | Question | Destination and core content |
| --- | --- | --- |
| Evaluator | What is Ashlar useful for? | Overview: category, value, graph illustration, current scope |
| First-time user | What can I inspect or try? | Start here: table definitions, synthetic fixtures, resolver candidate instructions |
| Model author | How does this represent my graph? | Concepts: identity, properties, endpoints, isolated nodes, unknown content |
| Active user | What behavior can I rely on? | Reference: publication/read/table/resolver contracts, enforcement matrix, qualified evidence |
| Contributor | Where can I help? | Source: existing code, tests and documented open integration boundaries |

Full-site documentation pages use a left section tree with current page,
parent and important siblings visible; a quiet right outline contains only
the current page's headings. Mobile retains every destination in a disclosure
menu. The homepage needs no sidebars; its section anchors are local shortcuts.

**Active-state convention:** the current page's global or section navigation
link carries `aria-current="page"`. CSS derives a blue underline and heavier
text from that attribute. Local section anchors do not impersonate page state.
Links have descriptive names; the first focusable link skips to main content.

## Visual Hierarchy

- Layout: warm paper background, a fine grid and restrained blueprint-blue
  accents. A small modular stone mark sits beside the Ashlar wordmark.
  Desktop content is capped at 1200px; 64px outer gutters become 24px on mobile.
- Hero: left-aligned category label, large editorial serif headline,
  plain-language description and two actions. The right side shows a synthetic
  typed graph inside a ruled diagram panel. Connectors and property annotations
  explain the model; decoration must never imply live data or measurements.
- Reading order: category and promise → graph → three concrete capabilities →
  schema example → current scope/evidence → next action.
- Emphasis: primary actions use solid blue; secondary actions use underlined
  text. Hairline rules group content. Reserve boxed surfaces for diagrams and
  code, rather than placing every paragraph in a card.
- Rhythm: 96px section gaps on desktop, 56px on mobile; 24px related-content
  gaps. Below 850px the hero stacks and capability columns become rows.

## Interaction States

| State | Applies to | Convention |
| --- | --- | --- |
| Hover | Links and primary actions | Underline or darker blue; no content movement |
| Keyboard focus | Every enabled link/control | 3px blue outline with 4px offset |
| Current page | Page navigation | Underline and bold text bound to `aria-current="page"` |
| Expanded | Future mobile documentation menu | Native disclosure or accessible button with visible label and `aria-expanded` |

The homepage prototype has links and anchors only. Loading, disabled, form
errors and empty search states are absent. Future search requires a separate
interaction design. Motion is unnecessary; reduced-motion preferences must be
honored if motion is added. No meaning depends solely on color. Diagrams have
text alternatives; code remains selectable.

## Tokens

### Color

| Token | Value | Use |
| --- | --- | --- |
| paper | `#F5F3EC` | Main background |
| ink | `#202B2D` | Headings and body |
| muted | `#526267` | Secondary copy |
| blue | `#234FBC` | Primary actions, diagram lines, active state |
| blue-hover | `#193A8C` | Hovered primary action |
| line | `#C8CEC8` | Decorative rules |
| panel | `#EAEDE5` | Diagram background |
| code | `#172A31` | Code background |
| code-text | `#E5F0ED` | Code text |

Body text uses ink or muted on paper. Primary buttons use white on blue.
Target text contrast is at least 4.5:1; large text and meaningful graphic
boundaries target 3:1. Decorative grid lines do not carry meaning.

### Spacing

Scale: 4 / 8 / 12 / 16 / 24 / 32 / 48 / 64 / 96px. Controls have at least
44px touch height. Avoid horizontal page scrolling at 320px and 200% zoom.

### Type

Display: Georgia/serif, 76px desktop and 46px mobile, 1.04 line height, normal
weight. H2: Georgia, 44/36px, 1.12. H3: system sans, 22px, 1.3, 600.
Body: system sans, 17px, 1.65. Small: 13px, 1.5. Labels: system mono, 11px,
1.5, uppercase with 0.12em tracking. Code: system mono, 13px, 1.7.
Use system fonts in the prototype so it works without third-party requests.

## Content and Evidence Boundary

Homepage headline: “Give your warehouse a graph.” Supporting copy identifies
Ashlar as a property-graph toolkit for typed entities and relationships on
warehouse tables, with Databricks Unity Catalog Delta as the first target.

Primary CTA: “Explore the table design.” Secondary CTA: “Understand the model.”
Use inspection language until an installable release and license are selected.
Do not invent an installation command, customer logo, endorsement or live demo.

| Public statement | Governing source | Treatment |
| --- | --- | --- |
| Domain-independent property graphs | PRD Summary and Open-source scope | Explain typed nodes/edges with synthetic TypeA/TypeB examples |
| Delta is the first target | PRD current milestone and physical direction | Name Databricks Unity Catalog Delta |
| Exact meaning and explicit enforcement boundaries | PRD FR-2 and data quality | Describe design responsibilities; link contracts |
| Physical-layout milestone closed in a scoped proof | Layout milestone acceptance, LAYOUT-AC1–6 | Link the record; omit headline benchmark numbers |
| Resolver candidate exists | `src/ashlar/README.md` | Identify candidate and local tests; no complete deployment claim |
| UMF integration | PRD owner revision | Explain UMF as a metadata/schema interchange fabric; mark binding as planned |

Reference pages must carry exact platform/profile/subset and evidence from their
source records. The homepage sends readers to those records without translating
synthetic feasibility into a general support promise. Production policy,
concurrency, retention, performance and billion-node qualification stay explicit.

## Design Review and Open Decisions

Review the prototype at desktop and mobile widths: category and actions are
visible first; diagram labels remain legible; all links are usable by keyboard;
the active cue matches semantic state; content and local anchors do not clip.
Full-site acceptance must include representative deep documentation pages,
mobile navigation, link checks, accessibility automation and screen-reader review.

Assumptions: platform engineers are the primary reader; a documentation-first
microsite is the intended conversion; the repository is the source destination.
Owner decisions: public domain/hosting, publication visibility, license and
release installation path. These do not block the local visual design.
Hugo + Hextra is the installed catalog pattern. ADR-002 selects native Hugo layouts
for this curated site; the owner selects GitHub Pages and Innsigle page signing.
Every authored page carries a source colophon after declaration approval.
Schema provides a scrollable graphical table map and field descriptions; Background
explains canonical/projection choices; Ecosystem shows native SQL, GraphFrames,
PuppyGraph, Fabric, Truss and intended UMF paths with qualified examples.

## Non-Goals

This artifact defines the interface system and editorial direction. Runtime
architecture, deployment, data flow, component internals and architecture-significant
decisions belong in their governing architecture, solution design, technical
design or ADR artifacts. The microsite is documentation; it does not add a graph
administration UI to Ashlar's product scope.
