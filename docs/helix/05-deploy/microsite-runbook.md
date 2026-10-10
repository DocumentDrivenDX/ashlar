---
ddx:
  id: ashlar.microsite-runbook
  type: runbook
  activity: deploy
  status: draft
  authoring:
    home: repo
---

# Runbook — Ashlar microsite

## Service Summary

Static documentation at `https://documentdrivendx.github.io/ashlar/`, owned by
the Ashlar repository maintainers. Degradation affects documentation readers,
not graph storage or publication. No on-call rotation has been supplied.

## Operator Entry Points

| Symptom | First check | Owner |
| --- | --- | --- |
| Publication failed | `gh run list --workflow pages.yml`; inspect failed job log | Repository maintainer |
| Page or stylesheet 404 | Pages deployment/environment and `/ashlar/` path | Repository maintainer |
| Missing/stale seal | Pinned Innsigle `verify --all`; Hugo warning log | Page author and signing-key custodian |

## Dependencies and Failure Boundaries

Hugo 0.167.0 and pinned Innsigle verification run in CI. GitHub Pages receives
the verified build artifact. The signing key stays in 1Password outside CI;
its absence cannot turn CI into an unsigned deployment. GitHub repository links
are external evidence destinations; a failure there does not invalidate local
navigation but requires content review.

## Alert Triage

Use workflow failures and a reader-visible 404 as initial signals. No recurring
monitoring or analytics service is configured. For latency, inspect document
and CSS requests; the site has no client API, warehouse query or authentication
flow. Web Vitals, real-user monitoring and page-error collection have no measured
baseline and must not be represented as passing production targets.

## Generated UMF runtime diagram

Before building, run `python3 website/scripts/check_runtime_model.py`. The CI
workflow refuses stale canonical model fingerprints or generated SVG/template
fingerprints. Regenerate and review through the pinned UMF checkout using
`tools/generate_runtime_diagram.ts`; the model README records exact commands
and supported semantics. This is a stale-output guard, not a signature or
native schema-enforcement claim. The shared template preserves the seven signed
page sources; generated content remains outside those source signatures.

## Common Incident Procedures

### Stale or missing source attestation

1. Inspect the exact changed source and prior declaration.
2. Obtain the required colophon approval; seal locally with the approved key.
3. Run CLI verification, strict Hugo build and `check_site.py --require-seals`.
4. Commit source and matching claims together; push for deployment.

Never bypass the CI seal gate to publish an edited source. Unpublished
transcript/provenance summaries stay local.

### Broken project links or navigation

1. Build with the production base URL and run the internal-link checker.
2. Fix template links using Hugo relative URLs, preserving `/ashlar/`.
3. Re-seal if page source changed; template/CSS-only changes do not alter the
   signed page source but require visual review.
4. Verify home, schema, ecosystem and a reference page on the live URL.

## Rollback and Recovery

Hold rollout when verification fails; the previous Pages deployment remains
the recovery surface. For a deployed regression, use a reviewed revert commit
containing the prior source and its matching attestations, then run the normal
workflow. Avoid destructive Git history changes and mismatched source/claims.
Confirm the deployment succeeds, assets load at `/ashlar/`, all core routes
respond and live source bytes verify with published issuer keys.

## Routine Operations

For each authored content revision: review declaration, seal, verify, build and
check routes before pushing. Reconsider the pinned tool versions deliberately;
never silently replace the verifier. Signing-key rotation requires publishing
the appropriate issuer history and reviewing retained claims with the custodian.

## Escalation and Communications

Escalate Pages/Actions permissions to the repository administrator and key
custody to the 1Password vault owner. Notify the Ashlar owner of a required
decision; no external notification channel or automatic messaging is configured.

## References

- [Deployment decision](../02-design/adr/ADR-002-signed-static-microsite.md)
- [Site build instructions](../../../website/README.md)


## UMF schema browser integration

The schema browser is embedded through a shared schema-page
partial and independently usable at `/ashlar/model/schema-browser/index.html`.
`tools/build_schema_browser.py` verifies the published UMF browser package before
retaining its exact renderer, stylesheet, license notices and all nine original model sources.
`website/scripts/check_schema_browser.py` verifies source/catalog/UI provenance
before publishing; CI runs it with the existing diagram guard. Rebuild through
a pinned release tarball as described in the site build instructions. No live warehouse or
external schema service is involved.

For each release update, compare the renderer with upstream main and run the
desktop/mobile browser checks for original source downloads, ordered logical
members, endpoint navigation and explicit partial Delta interpretation. Validate
the site’s navigation and seals before publishing. Keep actual run receipts under
build evidence. Generated/shared browser assets are outside the signed Markdown
sources; their package, source and output hashes are verified separately.
