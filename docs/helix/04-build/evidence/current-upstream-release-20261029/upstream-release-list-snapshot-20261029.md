---
ddx:
  id: ashlar.evidence.upstream-release-list-snapshot-20261029
  type: evidence
  activity: build
  status: draft
  authoring:
    home: repo
  links:
    - id: ashlar.evidence.r01-upstream-pin-refresh-20261029
      kind: informed_by
    - id: TD-007-native-xxhash64-lookup-layout
      kind: informed_by
---

# Official upstream release-list snapshot — 2026-10-29

Status: **published release-list snapshot confirmed by the official GitHub
releases API connector on 2026-10-29**. The newest published entries are UMF
`umf-v0.5.7` and `python-v0.10.3`, Weft `v0.4.56`, and Truss `v0.0.51`.
UMF's draft `qualification-g02-d5...` entry is excluded because it is a draft,
not a published release. This is a point-in-time release-list check; package
asset integrity and compatibility remain separate gates.

## Sources and selection

| Upstream project | Official release-list API | Newest published entry in snapshot | Ashlar pin |
| --- | --- | --- | --- |
| UMF | [GitHub releases API](https://api.github.com/repos/DocumentDrivenDX/umf/releases) | `umf-v0.5.7`; Python Core `python-v0.10.3` | Access 0.1.0.dev8; Core 0.10.3 |
| Weft | [GitHub releases API](https://api.github.com/repos/DocumentDrivenDX/weft/releases) | `v0.4.56` | 0.4.56 |
| Truss | [GitHub releases API](https://api.github.com/repos/DocumentDrivenDX/truss/releases) | `v0.0.51` | 0.0.51 in `truss-local` |

The UMF Python release notes state that Core 0.10.3 retains semantic Core
version 0.10.0. The runtime package version therefore does not change the
declared UMF document/profile semantics. Preserve the 0.10.0 profile identity;
do not infer a new schema profile from the 0.10.3 package pin.

## Remaining compatibility gate

Before qualifying the selected managed six-role profile, record the exact
published UMF profile/resource asset URL, release identity, byte size and
SHA-256; then verify its declared profile semantics against 0.10.0 and its
direct compatibility with the installed Core 0.10.3/Access dev8 cohort. This
release-list snapshot alone does not establish profile asset compatibility,
OSS-local realization, DDL/layout admission, or native execution. See
[R01 pin evidence](r01-umf-weft-pins-20261029/README.md) and
[TD-007](../../../02-design/technical-designs/TD-007-native-xxhash64-lookup-layout.md).
