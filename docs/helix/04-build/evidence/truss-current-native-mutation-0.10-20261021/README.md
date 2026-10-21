# Truss 0.44 / UMF 0.10 native mutation slice

This bounded local check used Truss toolkit 0.0.44 and PostgreSQL 18.6 with the
original 4082-byte `typed-graph-example` UMF 0.10.0 source. The accepted source
SHA256 was `19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b`;
the accepted report SHA256 is recorded in `result.json`. Before mutation, the
public report, accepted-document API and complete catalog view were read and
matched. Truss assigned Person/Place type IDs 1/2, property IDs 1–5, and the
`located` relationship ID 2.

Public group calls created Person, Place and an independent `located` edge,
updated Person quantity to the exact integer `9007199254740993`, updated the
edge order key, then deleted the edge and Place. Each group was read back and
its exact request replay was verified. Entity IDs and full receipts are retained
in the three receipt files; the selected request schedules are retained without
connection strings.

Complete feed admission remains unqualified. A consumer that starts at the
initial source cursor must traverse the earlier catalog-acceptance writer before
later mutation transactions. Advancing past it requires source ACK. The
single-writer experiment that could combine catalog acceptance and graph
mutations in one feed page stopped when Truss refused catalog acceptance from a
superuser-authenticated session switched to the ordinary role. No ACK was
issued because a protected Delta publication manifest/vector is not available.
The native servers were stopped after the runs.
