---
title: Give your warehouse a graph
description: Typed property graphs on Databricks warehouse tables.
composition: model-primary
---

<div class="hero">
<div><div class="label">Property graphs · Warehouse native</div><h1>Give your<br>warehouse<br><em>a graph.</em></h1><p class="intro">Typed entities. Meaningful relationships. Ashlar is a property-graph toolkit for warehouse tables, with Databricks Unity Catalog Delta as its first target.</p><div class="actions"><a class="button" href="start/">Explore the table design <span aria-hidden="true">↗</span></a><a class="text-link" href="concepts/">Understand the model</a></div></div>
<div class="diagram"><div class="diagram-head"><span>FIG. 01 / A TYPED PROPERTY GRAPH</span><span>SYNTHETIC</span></div>
<svg viewBox="0 0 480 360" role="img" aria-labelledby="graph-title graph-desc"><title id="graph-title">Typed nodes and property-bearing relationships</title><desc id="graph-desc">TypeA node A1 connects to TypeB node B1 through a directed relationship carrying confidence 0.98. B1 connects to TypeC C1. An isolated TypeA node A0 remains represented.</desc><defs><marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#234fbc"/></marker></defs>
<g fill="none" stroke="#234fbc" stroke-width="1.8" marker-end="url(#arrow)"><path d="M145 93 H286 V158"/><path d="M316 190 H385 V273"/></g>
<g fill="#f5f3ec" stroke="#234fbc" stroke-width="1.5"><rect x="26" y="56" width="122" height="74"/><rect x="225" y="157" width="122" height="74"/><rect x="323" y="273" width="122" height="74"/><rect x="26" y="264" width="122" height="74" stroke-dasharray="4 4"/></g>
<g fill="#234fbc" font-family="monospace" font-size="11"><text x="40" y="79">TYPE A</text><text x="239" y="180">TYPE B</text><text x="337" y="296">TYPE C</text><text x="40" y="287">TYPE A</text></g>
<g fill="#202b2d" font-family="Georgia,serif" font-size="28"><text x="40" y="112">A1</text><text x="239" y="214">B1</text><text x="337" y="330">C1</text><text x="40" y="320">A0</text></g>
<g font-family="monospace" font-size="10" fill="#526267"><text x="175" y="71">RELATES_TO</text><text x="160" y="115">confidence: 0.98</text><text x="322" y="251">RELATES_TO</text><text x="26" y="250">ISOLATED, STILL PRESENT</text><text x="26" y="179">identity</text><text x="26" y="198">type</text><text x="26" y="217">properties + provenance</text></g></svg>
<div class="diagram-foot">Nodes and edges carry properties.<br>Identity and source meaning stay explicit.</div></div>
</div>
<div class="strip"><span>FIRST TARGET</span><span>Databricks / Unity Catalog / Delta</span><span>Domain-independent by design</span></div>
<section class="section" id="concepts"><div class="label">01 / The model</div><h2>A shared structure for<br>connected data.</h2><div class="capabilities"><article><div class="number">01 — DEFINE</div><h3>Give every entity a place.</h3><p>Represent typed nodes, their properties and source identity. An entity stays in the graph even when it has no relationships.</p></article><article><div class="number">02 — CONNECT</div><h3>Keep relationships meaningful.</h3><p>Edges have types, direction, identity and properties of their own. Parallel relationships remain distinguishable.</p></article><article><div class="number">03 — ACCOUNT</div><h3>Make trust inspectable.</h3><p>Publication contracts describe what consumers can see, which revision they read and where integrity checks happen.</p></article></div></section>
<section class="section example" id="start"><div><div class="label">02 / Start with the structure</div><h2>Inspect a graph.<br>Follow it into tables.</h2><p>The schema package pairs Delta table definitions with synthetic examples, enforcement responsibilities and scoped graph mappings.</p><p>Start by reading the table contract. Then inspect the SQL package and the publication resolver candidate.</p><div class="actions"><a class="text-link" href="https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-003-delta-graph-tables.md">Read the table contract ↗</a><a class="text-link" href="https://github.com/DocumentDrivenDX/ashlar/tree/main/sql/ashlar-delta-v03">Inspect the SQL package ↗</a></div></div><div class="code-panel"><div class="code-title">A1 → B1 / CONCEPTUAL EXAMPLE</div><pre><span class="comment">// Illustrative data, not an Ashlar API</span>
{
  <span class="key">"node"</span>: {
    "type": "TypeA",
    "identity": "A1"
  },
  <span class="key">"relationship"</span>: {
    "type": "RELATES_TO",
    "identity": "E1",
    "from": "A1",
    "to": "B1",
    "properties": { "confidence": 0.98 }
  }
}</pre></div></section>
<section class="section scope" id="reference"><div><div class="label">03 / Scope &amp; evidence</div><h2>See what the design<br>is built on.</h2><p>The physical-layout milestone is closed for its documented synthetic scope. Table definitions, preservation checks and native lookup evidence are available for review.</p><p>Production qualification and performance targets remain open. Integration with UMF, a metadata and schema interchange fabric, is planned; its graph binding is deferred.</p><a class="text-link" href="https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/spikes/SPIKE-001-table-layout/layout-milestone-acceptance.md">Read the evidence and its limits ↗</a></div><div class="scope-list"><a class="scope-item" href="https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-003-delta-graph-tables.md"><span>Delta table design ↗</span><span class="status">SCOPED EVIDENCE</span></a><a class="scope-item" href="https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-001-publication-boundary.md"><span>Publication boundary ↗</span><span class="status">CONTRACT</span></a><a class="scope-item" href="https://github.com/DocumentDrivenDX/ashlar/blob/main/docs/helix/02-design/contracts/CONTRACT-002-consumer-read-boundary.md"><span>Consumer read semantics ↗</span><span class="status">CONTRACT</span></a><a class="scope-item" href="https://github.com/DocumentDrivenDX/ashlar/blob/main/src/ashlar/README.md"><span>Publication resolver ↗</span><span class="status">CANDIDATE</span></a><div class="scope-item"><span>UMF graph binding</span><span class="status">PLANNED</span></div></div></section>
<div class="closing"><div><div class="label">Build from an explicit contract</div><h2>Explore the foundations.</h2></div><a class="button" href="https://github.com/DocumentDrivenDX/ashlar">Open the repository <span aria-hidden="true">↗</span></a></div>

