# Cancelled installed medical read and Weft singleton run

One combined installed read/query attempt used exact commit `7f7092201f0a46609801f84d3e42bb7a9dd59658`, a fresh installed wheel and the original durable medical publication/source selections. The operator cancelled it after approximately 645 seconds. The owned capture reports `cause: cancel`, native phase `begun` and exit status 2; the outer harness terminated with status 2 after 645.777 seconds. This was not a successful native read or Weft query, and it does not measure singleton query latency. No final query result or read receipt was released.

Independent inspection of retained before/after inventories found exact equality across 127 original native/control files totaling 12389675 bytes, including unchanged SQLite SHM. This is byte/inventory correspondence within the selected 4096-file/128MiB bound, not a complete logical row-bag or source/ACK verification. Prior durable publication and ACK evidence remains [separate](../medical-held-writer-resume-20261016-r/README.md).

`capture-prefix-hashes.json` binds the exact retained stdout/stderr prefixes without copying the diagnostic warning stream. Query stdout was empty; retained query stderr was 676713 bytes. Prefixes are not asserted to be complete output. No raw diagnostic prefixes or sensitive source payloads are archived here.

Static analysis of repeated validation or native-call fanout can guide optimization, but does not attribute this run's elapsed time to any particular call, row count or component. There is no measured per-stage native latency claim in this archive. The next work is to reduce redundant validation and rerun a bounded functional check separately.

`manifest.json` binds 13 bounded configuration, preparation, harness, attempt and before/after inventory streams using original and gzip lengths/SHA256; gzip timestamps are zero. This evidence records cancellation, not query acceptance. No paid run is claimed.
