# Matched full-field ingest across file targets — R577–580

Two private2.5M-row fixtures receive the same6227 pinned fourth-input changes:
5609 updates/618 deletes, using the unchanged atomic full20field predecessor
guard. The64MiB fixture starts at32 files; the maintained256MiB fixture at16.
Complete21field initial parity passes over2498646 rows, complete11836 CDF images
match the independently qualified original reference, complete final21field
parity passes over2498028 rows and all final typed keys remain unique. Generated
fingerprints, exact property/retained tokens and typed endpoints are included.
No canonical table, real source ACK or production descriptor is written.

| Metric |64MiB candidate|256MiB candidate|
| --- | ---: | ---: |
| MERGE caller ms |7866.958500|6211.697083|
| Engine execution ms |7232|5553|
| Compilation ms |325|317|
| Metadata ms, nested |5486|4186|
| Read bytes |2354373885|2349345344|
| Remote read bytes |29934245|27571|
| Write bytes |14419345|14419345|
| Query-wide read files |67|34|
| Final active files |33|17|

Both guards retain exact missing/predecessor mismatch refusal behavior from the
qualified R421 builder; native mutation SQL is independently reconstructed byte
for byte in the audit. No fingerprint-only guard replaces complete equality.
Neither MERGE used result cache. Initial/closing histories bind exact statement
IDs and single-version mutation intervals. The final audit verifies38 native
statements, including one FAILED clone statement, exact SQL and final metrics.
Two immutable source-count preflight SELECTs were result-cached; these are not
uncached source timing evidence. Full content and atomic guard checks remain.

Databricks rejected a nested shallow clone of the existing256 fixture with
CANNOT_SHALLOW_CLONE_NESTED. R577 stopped before any MERGE, leaving a completed64
clone0 and no second clone. R578 uses that completed64 clone and the already-owned
256 fixture, enabling CDF at3 then mutating at4. Their selected before pins are
64:0 and256:3; final pins64:1 and256:4. Old256 pins0/2 remain preserved. No clone
or MERGE was blindly replayed. The exact failed handle was retired only after
native FAILED/is_final observation. The stopped runner remains immutable.

Combined original/resumed native costs:17321117479 read B,28838690 write B,0
spill, within30GB/1GB/0. Resume wall108.854981708s includes setup, native preflight,
mutations, complete preservation checks and telemetry. Original attempt/setup,
prior typed source materialization and prior256 maintenance are separately
charged evidence; no uninterrupted publication/freshness clock is claimed.

The256 candidate shows lower observed mutation latency with practically equal
read bytes, unlike its singleton sample's59.65% higher read bytes. Fixed order,
98% versus99% data-cache fractions, differing remote bytes and inherited file
geometry prevent a causal file-size claim. Query-wide file counts are not unique
physical target-file coverage, and nested metadata timings are not additive.
Prior256 maintenance itself took21108ms engine/1503890537 write B; a1.68-second
engine difference in one mutation does not establish amortization or sustained
policy admission. Generated-column fixture interoperability remains unqualified.

Keep256 as a physical candidate;64 remains the default pending combined evidence.
Next measure changed-key reads after these matched mutations, then use repeated
bounded paired mutations only if their cost and preservation bounds are justified.
A full-publication comparison must include source readiness, all graph roles,
concurrent closing custody, manifest/readback and counted maintenance. Caller
latency, controlled cold-data, production fence, sustained10k/s,100k/s burst and
actual1B/5B admission remain open. UC Delta and deferred UMF binding remain fixed.
