# Microsite design review

2026-10-08. Local design preview; no deployment or release approval.

- Desktop browser visual review: hero, graph labels and primary actions are
  legible, with no overlap in the initial viewport.
- Mobile browser check at 390px: document scroll width equals viewport width;
  layout stacks, navigation remains available and the diagram has a text alternative.
- Browser semantic check: Overview carries `aria-current="page"`; all local
  fragment destinations exist. Primary CTA navigates to `#start`; Overview
  returns to `#main`.
- Repository link targets exist locally. Remote publication/access has not
  been verified; the prototype points to the repository's `main` paths.
- `git diff --check` passes. Full accessibility, screen-reader, 320px/zoom and
  documentation-page checks remain implementation acceptance work.

The design-system template's required sections, concrete tokens and
attribute-bound current-page convention are present. Copy follows the revised
PRD and layout milestone acceptance: UMF binding is planned, the resolver is a
candidate, and production/performance qualification remains open.

## Full microsite verification

Seven authored routes build with Hugo extended 0.167.0. Production-output checks
pass local internal links and current-page semantics under `/ashlar/`. All linked
Ashlar repository paths exist locally. Actual browser checks at 320px show no
page overflow on home, concepts, schema, background, ecosystem, start and reference.
At 390px the schema panel scrolls its 688px contents within a 364px panel while
the page remains 390px wide. Desktop schema at 1440px exposes both documentation
navigation and the page-local outline. No live warehouse operation was run.

Signing declaration and key usage approved by the owner. All seven source seals
verify VALID, every claim matches the approved model-primary colophon exactly,
and the strict Hugo build and source-seal coverage checks pass. Live deployment
verification passes for all seven pages and published keys/claims.
The source-seal gate deliberately rejects unsigned/stale production builds.

## Live publication

GitHub Actions run 37731576687 completed build and deploy successfully. Live
URL: https://documentdrivendx.github.io/ashlar/. All seven page routes return
rendered source attestations. Every downloaded attestation matches its committed
claim and verifies VALID with the downloaded public issuer document against the
corresponding Markdown source. Live browser home-to-schema navigation passes.

The artifact is packaged explicitly to retain `.well-known/innsigle`; the default
Pages uploader excluded that directory on the initial deployment. Archive checks
now require the public issuer and all seven claims before upload.

Use per-file sealing with the reviewed `.innsigle/colo.json`; pinned `seal --all`
selects built-in example declarations and must not replace the approved colophon.
The homepage claim explicitly names the public root URL. Private key custody is
in 1Password; no private key is committed or provided to CI.
