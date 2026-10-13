# Original UMF 0.8 schema in the public browser

The latest published UMF schema-browser release was 1.1.2 on 2026-10-13;
Ashlar retains that exact pinned renderer and its original package hashes.
The catalog adds the original `examples/truss-native/core08-asset.umf.json`
bytes, including its declared 0.8.0 version and uninterpreted consumer vocabulary.
The previous 14 entries are unchanged.

The focused Chromium check passed at desktop and mobile widths: original source
DOM and download bytes match, unknown semantics are explicit, and the existing
eight logical runtime records retain their ordered member display. The retained
report names the selected schema; it does not claim all 15 downloads passed.
An earlier full-catalog attempt stopped on a 30-second download timeout at the
existing cohort-r2 entry, after ten downloads and before the new entry. Its cause
was not established; no renderer code was changed. The optional schema selector
keeps the focused check small and refuses unknown IDs.

Executed: `bun tools/check_schema_browser_browser.ts ASHLAR_ROOT UMF_SOURCE FRESH_OUTPUT truss-core08-asset`.
Static fingerprint and exact regeneration checks passed. Independent review
approved the catalog bytes, unchanged renderer, explicit unknown semantics and
selected-check reporting. Rendering declarations does not establish enforcement.
