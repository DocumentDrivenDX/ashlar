# Projected native detail component

One local Spark 4.0.1 / Delta 4.0.0 / JDK 21 component probe passed with six fresh
`DESCRIBE DETAIL` calls. The supervisor returned status 0 in 9.659455209 seconds,
with retained-leader cleanup confirmed. Spark closing completed; exact before/after
UUID, schema, synthetic properties, version-zero metadata JSON and no-add/remove
inventory agree. This is a fresh empty table, separate from publication fixtures.

| Pair | Execution order | Full detail seconds | Projected detail seconds |
| --- | --- | ---: | ---: |
| 0 | full, projected | 1.875460125 | 0.201675458 |
| 1 | projected, full | 0.270006333 | 0.186359417 |
| 2 | full, projected | 0.248636917 | 0.159148291 |

Each observation executes fresh native detail under unchanged bounded-capture
limits. All six UUID/three selected property values match the original synthetic
metadata; full-mode observations also retain the unknown property and location.
The observed full native row has 15 fields on this runtime. Timings cover these
individual calls only: there is no warmup, percentile, cache-control guarantee,
whole-workflow improvement or general latency claim.

The installed wheel is built from `33ddfb678fae565c4370591f095ec10fde38578d`;
492 installed members were compared byte-for-byte. The all-five-CI successor
`ec4e1744baae82aba5c032c6ae37bdbc638f44d7` changes only a test's explicit full-mode
selection. The execution clearance retains that reviewed mapping. The original
pending selection, reviewed pending selection and operative selection are retained;
reviewed-to-operative changes only `ready: false` to `ready: true`.

The component constructs no journal, registration owner, reservation authority,
NativeDriver, source, ACK or publication. Synthetic registration property equality
is component evidence, not original installation authority qualification. Existing
registration/reservation refusal tests remain separate. The run neither completes
nor retries the full publication workflow.

[manifest.json](manifest.json) pins original bytes and deterministic gzip streams
(`mtime=0`), including scripts, preparation/provenance, bounded terminal streams,
results and the exact initial Delta metadata log. No binary, environment dump,
SDK corpus or business dataset is archived.
