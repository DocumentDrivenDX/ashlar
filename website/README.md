# Ashlar microsite

Build with Hugo extended 0.167.0:

```sh
hugo --source website --destination public
python3 website/scripts/check_site.py website/public
hugo server --source website --baseURL http://localhost:1313/ashlar/
```

`content/` contains seven authored pages; shared layouts render the design system.
The schema page adds a UMF-generated runtime ER diagram and complete expandable
field lists through a shared template. Its input/output fingerprint check is
`python3 website/scripts/check_runtime_model.py`; generation and actual-UMF
integration commands are documented under
`docs/helix/02-design/models/ashlar-delta-runtime/README.md`. The separately
retained conceptual SVG is authored in the signed page source. Both have text
alternatives; generated template/assets are outside source-signature coverage. The
ecosystem examples link exact local evidence and distinguish deployment gaps.

Innsigle signs each Markdown source, including embedded diagrams. Its Hugo
integration re-hashes the source before displaying the colophon. The signature
does not cover generated HTML or shared layout/CSS bytes. The issuer and claims
publish under `/ashlar/.well-known/innsigle/`. Private keys stay outside Git and CI.

After an approved source/colophon update, seal each source individually with
`innsigle seal website/content/PAGE.md --colo .innsigle/colo.json --uri PUBLIC_PAGE_URL`.
Do not use `seal --all` for these pages: that command selects built-in example
colophons in the pinned CLI instead of the reviewed declaration.
Seal with the pinned Innsigle CLI,
then verify and rebuild. CI verifies committed claims and requires one rendered
attestation per page; it refuses stale or missing seals. It never silently signs
new copy. No session transcript/provenance journal is published.

The workflow builds pull requests without deployment. Main pushes and manual
runs build and deploy through the `github-pages` environment. GitHub Pages must
use the Actions publishing source. Expected URL:
https://documentdrivendx.github.io/ashlar/.

## UMF schema browser

The schema page embeds UMF’s actual browser, pinned at
`c433cfcdde21995803aad65234f20ba95d8c3222`, with a standalone view at
`model/schema-browser/index.html`. The upstream JS/CSS remain byte-identical.
Ashlar owns the surrounding HTML/CSS and a catalog containing the exact eight
physical UMF definitions plus the logical fields/keys/relationships model.
It loads only local assets; local-file inspection and source download remain
browser operations. Delta extension interpretation is explicitly partial.

```sh
python3 tools/build_schema_browser.py /path/to/umf
python3 tools/build_schema_browser.py /path/to/umf --check
python3 website/scripts/check_schema_browser.py
```

The build reads immutable Git objects at the pinned UMF revision, without
changing its working checkout. Review the generated catalog and provenance after
model changes. CI checks original model, bundle, host and catalog hashes before
publishing. Generated/shared browser content is outside the seven Markdown
source signatures, like the existing generated diagram.
