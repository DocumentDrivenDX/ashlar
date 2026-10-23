# Truss 0.0.49 R4 current-UMF offline readiness packet

R4 is a fresh, unapproved successor packet for the current Ashlar main and
UMF candidate bridge profile. It supersedes R3 only for current-upstream
qualification. The earlier R3 receipt and proposal remain unchanged historical
evidence; R3 used UMF access dev6 and the private 0.5.6 bridge predecessor.

The detached Ashlar source snapshot is pinned to `f5e89926e553b1887989b770d30e252e505316f5`.
The bridge readiness pin selects public-main-derived UMF candidate
`9a11ae5c04c23f0cc0ae1d61a27c4d7a64819b08`, IIFE SHA-256
`b686d8aa7ce55047f2c5bd9269a39cbd3ca43228ce70ae738a207108da200ce5`,
realization manifest `6fe91c59732342314263728c13114660e43f9150be8797639b5897fb9fe4c6b3`,
build-input inventory `9ac1f3c0412a7f4b617e15ff0581b1e424fac9d6da0d8aeb4535371ee550feff`,
and role semantics `1844cd4f2e3501e546aeb54327948b87d0860dd1e330d4acc1c9ed56a075b5f4`.
This is an offline proposal bridge, not native Delta qualification.

The selected Python cohort is Truss `0.0.49`, UMF core `0.10.0`, access
`0.1.0.dev7` release wheel SHA-256
`a78053ab8f35f55f23661de780f351f90e71299773774b62938ffb09270f3597`, and
Weft `0.4.52`. The exact 27-wheel CPython 3.12.14/macOS arm64 lock is
`/private/tmp/ashlar-r4-cp312-wheelhouse-20261023/wheelhouse-lock-pyspark-4.2.0-attempt-02.json`
(SHA-256 `d2686e1dbbdd5a136990b146b25375cba5a7c73e21ef30290cdfa12b3c70aa20`).
The fresh campaign runtime was installed offline at
`runtime/venv`. The install receipt is
`runtime/runtime-install-attempt-20261023.json` (SHA-256
`c8e3056973fbea931dda98c47f1b67fc4ba651583e148183f281a2e0058cae55`). All 27
distributions match the locked versions and wheel hashes; `pip check` passes,
and every installed RECORD payload was rehashed. Focused imports and public Truss API signatures were captured in
`runtime/runtime-preflight.json` (SHA-256
`6a9e35679a73350646e5e8af32be67b0c9b2eb30e2a5d124f3dcbe758a6250df`). Its
scope is package provenance and import compatibility only. No campaign worker
or supervisor preflight has run, and no PostgreSQL, Spark, source, feed, ACK,
managed Delta or Unity Catalog qualification is claimed.

The original R4 venv was contaminated: its `.pth` file added R3 and older
site-packages. It is preserved as `runtime/venv.quarantined-contaminated-20261023`;
its configuration and `.pth` contents are recorded in
`runtime/runtime-install-attempt-20261023.json` (SHA-256
`c8e3056973fbea931dda98c47f1b67fc4ba651583e148183f281a2e0058cae55`). The
fresh venv has no `.pth` files or external site-packages on `sys.path`, and no
UMF dev6 metadata. The cleanup-only `pg_ctl` is now pinned to the selected fresh
`pgserver-pg18==18.6.1` installation at
`runtime/venv/lib/python3.12/site-packages/pgserver/pginstall/bin/pg_ctl`,
SHA-256 `59b641aabae44b43acfc34fd0bdeb9af9480338d39b25f3088d7c486fdd8b999`;
the binary was not executed.

Copied R3 process-inventory and SysV observations are retained as historical
context only. Current R4 admission gates remain pending until an effect-free
supervisor preflight is explicitly run after the local resource slot clears.
No approval file, start marker, or campaign execution is staged. Execution still
requires a fresh owner approval after independent review of the exact packet
hashes.

The fresh private root is
`/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r4-f5e89926`; its
detached source clone is
`/private/tmp/ashlar-truss049-r4-source-f5e89926`. `attempt-r4` is empty.
No approval proposal, execution approval, or start marker is staged. Planned
limits remain one attempt, a 600-second child deadline, 1 GiB combined
retained/attempt/log bytes, 16 MiB per worker log, and zero retries. The
preflight must remain effect-free; campaign execution requires a later fresh
owner approval after independent review.
