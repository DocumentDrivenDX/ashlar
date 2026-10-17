# Configured models in the public schema browser

The catalog includes 18 exact original sources, adding the configured scalar
revision 1, separately authored named scalar revision 2 and Person/Place graph.
The original UMF 0.8 model bytes are retained in source and download views. Public
`schemaFormat: umf` selects the ontology renderer for these entries.

Existing Chromium 153.0.8010.12 checks at 1440 and 390 pixels verify the graph's
two Records and two authored relationships, exact source/download bytes, hash
navigation and no horizontal overflow. Screenshots retain the Person self-loop
and Person-to-Place relationship. The same selected check preserves the eight
runtime Records' ordered members and page lifecycle. An initial check exposed
the missing public format hint; the corrected check passed. This qualifies the
selected graph rendering, not native ingestion or every schema/browser feature.

The seven existing/plain sync test functions, including the new configured-model
source-drift refusal, ran directly with real temporary directories because pytest
was unavailable. Generator `--check`, the owning static checker and diff checks
passed. The stale test fixture now copies all declared original source paths.
Root review checked the six-file change and rendered ER image independently.

Public npm latest metadata observed on 2026-10-17 reports version 1.1.2. The
existing 2,290,558-byte package SHA256 remains
`45d559a5d0d4ff22826fb7f00211ea8c320823a6f3aaf403df744c4d0086cd34`;
all 85 upstream package files are unchanged. No new dependency was installed.
The metadata observation is time-bound, not an automatic future freshness claim.

The manifest binds the exact report, two screenshots, npm metadata and checked
source hashes. Gzip timestamps are zero. Unrelated site source edits were excluded.
