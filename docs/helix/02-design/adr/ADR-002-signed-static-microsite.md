---
ddx:
  id: ADR-002
  type: adr
  activity: design
  status: accepted
  authoring:
    home: repo
  links:
  - id: ashlar.prd
    kind: informed_by
---

# ADR-002: Source-sealed Hugo microsite on GitHub Pages

| Date | Status | Deciders | Confidence |
| --- | --- | --- | --- |
| 2026-10-08 | Accepted direction; signing declaration approved | Ashlar owner selects full microsite, Innsigle and GitHub Pages | Local Hugo build and source integration inspected; live deployment still to verify |

## Context

Ashlar needs a public microsite with a schema graphic, explanation of structural
choices and ecosystem mappings with examples. The owner explicitly selects
Innsigle page signing and GitHub Actions deployment to GitHub Pages. The existing
design-system instance supplies the visual language. Core runtime data and
credentials must remain outside the site.

## Decision

Build a static Hugo extended 0.167.0 site under `website/`, using shared native
layouts and authored Markdown. Publish only `website/public` through GitHub's
Pages artifact/deployment actions. Innsigle 0.6.1 at pinned source revision
`4185eb56beb7b52beaa4a00c2fc897f93d44b939` supplies source-seal verification and
the Hugo colophon integration. The revision exists in the upstream repository.

Seven directly reachable destinations serve evaluation (Overview), model
orientation (Concepts), physical understanding (Schema), rationale (Background),
consumer decisions (Ecosystem), onboarding (Start here) and exact lookup
(Reference). The left documentation tree exposes siblings; the right outline
contains current-page headings. Navigation styling derives from semantic
current-page state. Mobile preserves every link and scrolls the detailed schema
within its own panel.

Sign Markdown source, including its inline diagrams, after colophon review.
Publish the issuer/attestations at the stable project subpath. Keep private keys
outside Git and CI. CI verifies committed seals, requires one rendered
attestation per page and refuses stale/unsealed content. It does not sign new
copy automatically. The signature does not cover rendered HTML, layouts or CSS.

## Alternatives

| Option | Benefits | Costs | Disposition |
| --- | --- | --- | --- |
| Hugo + Hextra | Catalog pattern with built-in navigation/search | Would replace the established editorial/diagram layout or require theme overrides | Deferred; reconsider when reference volume requires search/hierarchy |
| Standalone duplicated HTML pages | Minimal build dependencies | Repeated navigation/layout and weaker source-content organization | Rejected for multi-page upkeep |
| Native Hugo layouts and Innsigle partials | Preserves approved direction, shared layout, static output and source seals | Own responsive/nav tests; no search yet | Selected for seven curated pages |

## Consequences

No client runtime or external font service is needed. Page changes require
reviewed re-sealing. An invalid source seal blocks publication while the prior
deployment stays available. GitHub Actions needs Pages write and OpenID token
permissions only in the deployment job; pull-request jobs cannot deploy.

Hugo/templates remain part of the build trust boundary. Source provenance is
separate from product support, evidence correctness and production security.

## Risks and Validation

| Risk | Likelihood / impact | Mitigation and review trigger |
| --- | --- | --- |
| Project-subpath links escape `/ashlar/` | Medium / broken navigation | Production-output checks and live URL verification |
| Edited content carries a stale seal | Medium / false provenance | CLI verification and strict Hugo warnings block deploy |
| Mobile schema labels become unreadable | High / poor usability | Scrollable diagram panel, text alternative and mobile browser inspection |
| Source/runtime claims drift | Medium / misleading readers | Link exact qualified evidence; review pages with governing changes |

GitHub Pages configuration and successful live deployment are not assumed.
Repository visibility and administrator/push permissions were read through the
GitHub API. Local build and route checks pass; record signing/live verification
in the deployment evidence before claiming completion.

## Concern Impact

Apply the installed product-microsite-ia practices for reader paths, explicit
navigation state and proof links. Hugo + Hextra remains a library candidate;
this site selects native Hugo layouts instead and records that trade-off here.
Core data concerns still constrain copy and diagrams. No graph administration
UI or additional runtime support is introduced.

## References

- [Interface system](../DESIGN.md)
- [PRD](../../01-frame/prd.md)
- [Innsigle Hugo integration](https://github.com/DocumentDrivenDX/innsigle/blob/4185eb56beb7b52beaa4a00c2fc897f93d44b939/integrations/hugo/README.md)
- [GitHub custom Pages workflows](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
- [Hugo Pages deployment](https://gohugo.io/host-and-deploy/deploy-to-github-pages/)
