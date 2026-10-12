# Public Truss schema acceptance

On 2026-10-12, `examples/truss-native/accept-schema.py` ran successfully against
a fresh Ashlar-owned local PostgreSQL 18.6 installation using the existing
Truss Python package, psycopg 3.3.6/libpq 18.6 and Bun producer. No package or
database build, shared server, runtime inventory or full suite was involved.

The unchanged original `cohort` UMF 0.7.0 fixture was selected from Truss's
committed relational fixture at `559f3cda`. Public installation returned the
registrar role; only the new cluster's ordinary consumer received that role.
The example committed registration `9f03446b-3cf4-4f79-8c0e-caa186e84ee4`, revision
`1`, then read the complete report and catalog in a new transaction. The report
contains two types, four properties and one relationship; the catalog contains
11 definitions at revision `1`. Original bytes are retained in `report.json`,
`catalog.json` and `source.umf.json`.

The one run completed in 8.359 seconds with exit 0, after connection and server
cleanup. The owned data directory's postmaster marker was absent afterward.
No retry was needed. This proves the small public accepted-schema/catalog
workflow, not mutations, schema evolution, Truss feed/ACK, UMF 0.8 pack support
or end-to-end Delta ingestion. Those P10 outcomes remain required.
