# Ordinary Fabric HTTP functional check

The separately configured ordinary transport was exercised on 2026-10-12 using
the already signed-in Homebrew Azure CLI and its user-owned `0755` configuration
directory. No directory permissions, credentials, capacity or graph definitions
were changed, and no SDK/runtime inventory was performed.

Authentication acquisition and closing account validation completed successfully
in 2.517 seconds. One subsequent read against the existing `Ashlar_R1` model used
the same owner with a 16 KiB response/aggregate limit, one-request limit and
10-second socket timeout. It completed and closed successfully in 2.546 seconds:

```gql
MATCH (n:AshlarNode) RETURN count(n) AS node_count
```

The endpoint returned HTTP 200, native status `00000`, and a TABLE with a
`node_count` UINT64 column and one row containing `3`. The original request body
SHA256 was `881a84c7fa3bd731c8ec4cade04496e94b20242634320a189ec3c4012ddb8cc7`.
No continuation or retry was required; both processes completed with exit 0.

This proves the selected ordinary authentication and query transport path for
this small observation. It does not prove original source availability,
publication-to-loaded-graph correspondence, no mixed refresh, compiler query
coverage, arbitrary account/runtime compatibility, or the full P07 corpus.
The canonical `query-fabric-gql` command still requires its actual release policy.
