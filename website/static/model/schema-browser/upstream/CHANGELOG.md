# Changelog

## 1.1.2 — 2026-10-11

Fix light/dark diff and annotation contrast, relationship-number paint and direct
component theme tokens. Reserve space for multiple map annotations, expose record
change states in text, and make source panes keyboard scrollable. Selecting a
pack-linked schema no longer fetches unselected manifests or ontology companions;
metadata-dependent details remain explicitly unavailable until their source is
selected. Clarify the tested CSP baseline and optional download/image permissions.
Mount options, catalog version and source preservation contracts are unchanged.

## 1.1.1

- Disable identifier minification to remove context-dependent generated names in Bun 1.3.14; retain syntax and whitespace minification.
- Pin the 64-package fixed validation profile to versioned, hashed release inputs. New extensions remain preserved without an unsupported validation claim.
- Keep exact released runtime and exported API byte checks.


## 1.1.0

Versioned catalog/host contract; correlated selection and navigation/error events;
strict-CSP fixed validators; lazy YAML/native recovery; custom categories, themes,
annotations, bounded lazy sources and revisions; independent property/map/diff
components; immutable source comparison; standalone producer build and complete
runtime-closure microsite parity. Adds hideHero, focus(), themeVariables and content-height events; removes the base element for base-uri policies; readiness now rejects on startup timeout. Catalog version remains 1 with optional additions.

## 1.0.0

Initial isolated ESM mount, inline entries/catalog URL, standalone assets,
table/artifact/ontology workspace and dual MIT/Apache-2.0 licensing.
