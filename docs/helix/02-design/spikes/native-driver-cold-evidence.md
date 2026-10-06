# SQL-driver remote-I/O screening

Cache-disabled persistent SQL driver 4.3.0 on existing shared serverless Photon
2X-Small dbw-aidev-cus compute, SQL channel 2026.38. Two new 1M-object liquid-id
fixtures use 16MiB and 128MiB target files. First data reads occur after creation
and OPTIMIZE, before full data validation. Each shape has 32 requests targeting
varied-property IDs; native metrics qualify remote I/O. Full outer-join property
and retained-text equality against the fixed source then passes before repeats.

| Layout / phase | Requests | Remote-I/O requests | Caller p95 all / remote subset ms | Engine p95 all ms | Remote bytes total |
| --- | --- | --- | --- | --- | --- |
| 16MiB first touch | 32 | 27 | 698 / 698 | 502 | 654,494,108 |
| 128MiB first touch | 32 | 9 | 837 / 1218 | 602 | 914,174,766 |
| 16MiB repeat | 32 | 0 | 352 / — | 87 | 0 |
| 128MiB repeat | 32 | 1 | 335 / 652 | 89 | 135,163,392 |

All 128 queries have zero result-cache hits and one read file at the median.
The 16MiB remote-I/O subset passes <=1s caller screening; the 128MiB subset
fails. Twenty-seven and nine cold-qualified samples do not establish production
population tails, and the subsets are not a matched set of independently cold
files. Remote I/O can coexist with cached bytes. One nominal repeat reads remote
bytes, demonstrating that a repeat label alone does not prove warmed data.

This supports advancing 16MiB liquid files as a native-singleton candidate,
subject to write amplification, maintenance, file-metadata scale and concurrent
read evidence. It is not a production file-size choice or billion-node admission.
The <=100ms warm engine target passes this screening; <=250ms warm caller still
fails from the local benchmark host. No in-region caller timing is inferred.

Raw evidence: `SPIKE-001-table-layout/out/native/ashlar_cold_20261005_d2/`,
138 completed statement metrics and exact carrier equality assertions. Harness
`native_cold_driver.py`; summary `summarize_cold.py --run ashlar_cold_20261005_d2`.
No compute setting or shared cache was changed; synthetic fixtures remain.
