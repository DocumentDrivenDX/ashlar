# Prepare original UMF documents through Truss

Use the installed `ashlar prepare-truss-catalog --config CONFIG` host command
with the closed configuration in `configuration.example.json`. Replace every
placeholder with an explicit selected path, byte length and SHA-256. The example
refuses until those selections describe actual files; it supplies no default
profile, package installation or credentials.

Select an immutable `truss-toolkit` wheel and its independently verified source
revision. Install it and its declared dependencies into explicit private import
roots. Describe every file and directory in those roots, including the selected
Truss distribution metadata. Installed Truss payloads must match the wheel and
its original RECORD. A selected Python host at least 3.11 and release-matching
Bun execute the preparation; Ashlar's portable core keeps its own Python profile.
The command uses `-I -B -S`, disables site startup and imports only selected roots.
It neither installs packages nor substitutes a sibling checkout.

List original document IDs, revisions, optional artifact identities and unchanged
UMF source files in their intended order. Preserve the declared UMF version in
those bytes. Supply a separate exact Truss AcceptanceInput configuration file
without `documents` or `interfaceVersion`: its layout, acceptance, validator,
support, binding, policy and transforms choices come from your selected Truss
profile, not this example. A profile pin is correspondence, not authentication
or an installed authority grant. Choose limits large enough for the complete
original request representation and all result views while respecting their
contractual ceilings. The private output parent must exist with mode0700; the
output child directory must be absent.

The result preserves original Truss configuration bytes in
`original-configuration.json` on prepared and refused outcomes, original source bytes, input, declarations, archived
sources, complete observations, report evidence and producer provenance as
separate files with a bounded index. The receipt distinguishes the original Truss
configuration digest from the outer Ashlar command configuration digest. Unknown content, incomplete interpretation
and unavailable owner operations remain visible. Unsupported declared versions
retain their original refusal diagnostics and do not become successful
preparations. Failure may leave explicitly incomplete evidence; a successful
receipt is released only after complete child transport and closing custody.

This operation opens no database and creates no accepted schema ID, catalog head,
source authority, checkpoint, feed, ACK or publication. The result is a preparation
handoff for later independently authorized catalog acceptance. It does not
complete P10 Truss streaming integration. See
[TD-004](../../docs/helix/02-design/technical-designs/TD-004-truss-catalog-preparation.md)
and [CONTRACT-005](../../docs/helix/02-design/contracts/CONTRACT-005-schema-intake.md).
