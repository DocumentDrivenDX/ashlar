# Schema browser host contract 1.1.0

Catalog version 1 is defined by `catalog.schema.json`. The browser is a read-only
source inspector, not an instance validator, native execution engine or document
viewer. Native source text and unknown metadata remain inspectable unchanged.

## Catalog and sources

Supply exactly one of `catalog: {version:1, entries, categories?}`, `entries`, or
`catalogUrl`. Each entry has unique `id`, `title`, `category`, `path`, `format`
(`json` or `yaml`) and exactly one `text` or `url`. Aliases cannot collide with IDs
or each other. `schemaFormat` explicitly selects `umf`, `tablespec`,
`artifact-collection` or `json-schema`; source shape remains checked. `pack` and
`packVersion` link catalog entries. Assets have id, reference, URL, format,
dataKind, sha256 and optional revision; links do not acquire originals.

Limits: 10,000 entries, 8 MB catalog transport, 4 MB per source/local file,
100 revisions per entry, JSON nesting 128 and 100,000 values. Responses are read
with byte bounds. URLs require HTTP(S), no URL credentials and no redirects.
Same-origin cookies are permitted; cross-origin requests omit cookies and need
CORS. Relative source URLs resolve against the catalog URL, or `assetBaseUrl`
for inline catalogs. Asset links resolve against `assetBaseUrl`. Sources load
only when selected; search covers metadata and already loaded/inline source text.
Unloaded source text is not searched or implicitly fetched.

Categories are `{id,label,order?}`. Entry/mount annotations map exact IDs to
`[{badge,tone?,tooltip?}]`; tones are neutral/info/positive/warning/negative.
Annotations are text, carry host claims, and do not modify the source.
Revisions are `{id,label?,format,text|url}`. Originals remain independent of the
selected revision. Structural comparison preserves unknown metadata and exact
numeric tokens; it makes no semantic compatibility or migration claim.

## ESM mount and lifecycle

`mountSchemaBrowser(root, options)` (alias `mount`) requires `assetsUrl` and a
catalog source. Options include `categories`, `selection`, `initialRoute`,
`annotations`, `readOnlyLocalFiles`, `theme` (light/dark/auto), title and height.
The mount renders inside an isolated iframe; root-global IDs and stylesheet
leakage are avoided. Serve the complete assets directory, including lazy chunks.

Callbacks: `onReady`, `onSelect(selection)`, `onNavigate(selection)`,
`onCatalogError(error)` and `onError(error)`. `ready` resolves when the catalog
and UI initialize, independently of selected-source completion. Catalog errors
reject it. `select({schema,definition?,relationship?,revision?})` returns a Promise
settled when the selected source is rendered or fails. `destroy()` removes the
frame/listener and rejects pending selections. `setAnnotations` replaces mount
annotations. Handle rejected promises when unmounting during initialization.

## Routing and iframe protocol

Hash grammar is URLSearchParams: `#schema=<entry-id>&definition=<definition-key>`;
optional `relationship`, `focus`, `revision` select the relevant views. Definition
keys are JSON arrays `[moduleId,elementId]`; relationship keys are
`["relationship",moduleId,relationshipId]`. Percent-encode using URLSearchParams.

Standalone iframe hosts can use `assets/index.html?parentOrigin=<origin>&instance=<id>`.
The configured origin is the allowlist; it must be an exact HTTP(S) origin.
The browser accepts messages only from its parent Window at that origin, with
matching instance and protocol 1. It never trusts the first message sender.
Envelope: `{protocol:1,instance,type,payload,requestId}`. Incoming types are
`umf-explorer:focus`, `umf-explorer:select`, `umf-explorer:annotations` and `umf-explorer:configure`.
Configure takes catalog/entries/catalogUrl, categories, selection, annotations,
readOnlyLocalFiles, theme and assetBaseUrl; the bootstrap trust settings remain
fixed. Configure supersedes outstanding catalog/source operations; only the latest validated catalog commits. Omitted annotations, categories, theme and local-file settings reset to defaults. Success emits ready and then navigate/select with the supplied requestId; failure emits a correlated CATALOG error. Standalone query options also include catalogUrl, assetBaseUrl, theme and
readOnlyLocalFiles=true. Outgoing types are
`umf-explorer:height` (payload `{height}`), `umf-explorer:ready`, `umf-explorer:select`, `umf-explorer:navigate`,
`umf-explorer:error`. Selection responses preserve requestId. Errors include code,
phase, message and available HTTP status/URL/requestId. Hosts must likewise check
Window source, exact origin, instance, protocol and payload. Never use `*`.

## Styles, components and CSP

Iframe assets use local system fonts and CSS tokens `--paper`, `--ink`, `--muted`,
`--line`, `--umf-font`, `--umf-mono`, `--umf-space`, plus surface tokens in
explorer.css. Theme selection stays inside the frame. Direct component styles
are shipped as `assets/components.css` and use `umf-` class prefixes and `--umf-paper/ink/line/focus` variables; import their
stylesheet only where desired.

Exports include `renderPropertyTable`, `renderRecordMap`, `compareDocuments` and
`renderDocumentDiff`; renderers return destroy handles and use root-owned DOM,
text content and callback navigation. Model nodes/edges have explicit IDs,
labels, created/changed/removed status and annotations.

JSON inspection uses fixed build-time validators and no `eval`/`new Function`.
A same-origin host can use `default-src 'none'; script-src 'self'; style-src 'self';
connect-src 'self'; frame-src 'self' about:; img-src 'self' data:`. No inline
executable scripts or remote fonts are required. Srcdoc inherits host CSP; host
assets/cross-origin catalogs need explicit origins. Blob downloads may require
host download/sandbox permission. Optional native recovery can refuse grammars
outside the fixed browser validation profile with an explicit error; it never
falls back to a permissive check. Core library validation remains unchanged.

Browser qualification and exact tested versions are recorded in release evidence;
Chromium evidence does not establish cross-browser support.

`hideHero: true` removes the introductory hero. `focus()` moves keyboard focus to the current inspector heading; Tab then follows its links and controls in DOM order. Leaving the frame returns to the host's next focusable element. Use `height: '100%'` in a container with an explicit height to fill a flex area; `onContentHeight(height)` reports content height for hosts that choose automatic sizing.

`themeVariables` accepts `--paper`, `--ink`, `--muted`, `--line`, `--accent`, `--umf-font`, `--umf-mono` and `--umf-space`. These are applied through the CSSOM after theme selection; hosts are responsible for maintaining contrast with custom colors. The srcdoc document has no base element and works with `base-uri 'self'` or `base-uri 'none'`.

Avro recovery preserves schema, dependencies and exact source while reporting AVRO_VALIDATOR_LIMIT: avsc dynamic interpretation is deliberately unavailable in the fixed browser profile. Source, schema and dependencies remain preserved; native capability diagnostics can differ from the native runtime. The qualified fixture differs only by this warning. No runtime code generation is attempted. Other native capability warnings likewise remain visible; source recovery does not claim arbitrary semantic validation.
