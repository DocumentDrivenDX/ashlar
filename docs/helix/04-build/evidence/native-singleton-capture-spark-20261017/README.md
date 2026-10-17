# Native Spark singleton capture

One reviewed, bounded local component probe passed using exact commit `84478e9965f673e39e42da18396091b06d2691d8` and wheel SHA256 `b8ee538ec4f103d4af4ff35856ad49992164b0a8c8e76e2f30823fa93fe837e2`. The fresh isolated overlay's 489 installed package/resource files were checked against the wheel. It reused Python 3.11.17, Spark 4.0.1, Delta 4.0.0, JDK 21 and the existing cached JARs, with local[1] and 512 MiB driver/executor selections. No dependency download or shared-runtime modification was needed.

Six guarded capture actions passed:

| Case | Actual outcome |
| --- | --- |
| `DESCRIBE DETAIL`, maximum rows 1 | Original typed singleton returned |
| Same original detail frame, maximum rows 2 | General guarded path returned identical schema and typed values |
| Empty SQL frame | Exactly `[]` |
| One all-NULL row | Struct, date, decimal, map and array columns remained five distinct typed NULL cells in one retained row |
| Two-row singleton input | `LocalDeltaError`, no returned payload |
| Oversized UTF-8 cell | `LocalDeltaError`, no returned payload |

The detail result preserves actual native datetime/map/array carriers through an explicit typed reporting codec. The all-NULL case verifies row preservation and declared nested/date/decimal/map/array schemas; it does **not** qualify non-null decimal or populated nested-value payloads. These are direct capture-component calls on actual Spark frames. The component-only transport object has no journal, source sessions, publisher, policy or installation authority.

A fresh probe-only Delta table was created by SQL with `id BIGINT NOT NULL`. Its actual UUID `ce26e76a-3e0d-4a03-a3b6-e63ad37130e9`, exact schema and sole version-0 metadata JSON were unchanged before/after. The retained native log contains a CREATE TABLE commit and no data-add/remove actions: no business rows were written. The probe's empty Delta directory remains outside this evidence archive; existing installations were not used as fixtures.

Whole supervised workflow elapsed **7.760042708 seconds**, including Spark startup **2.254300417 seconds** and closing. The singleton/general capture observations were **0.446374458 / 0.545221000 seconds**, in that fixed order on the same frame. No cache controls, repeated samples, ratio or speed guarantee follow. `spark_sql_call_seconds` separately records the call returning each frame; `guarded_capture_seconds` includes native guard execution and transfer. Neither is an invented split of raw SQL execution from lazy evaluation.

The supervisor reported status 0, successful Spark closing and confirmed absence of its selected owned process groups after reaping session leader 74854. Work was bounded to 60 seconds, with a five-second termination grace, bounded owned cleanup and 1 MiB stdout/stderr limits. The six capture actions are separate from the CREATE/DESCRIBE metadata commands. This is not a source/ACK/publication, old-publication verification, production, scale or full-workflow pass.

`manifest.json` pins 18 lossless gzip archives with zero timestamps: exact controls, preparation and execution clearance, wheel/source provenance, original result, before/after metadata, native version-0 log, terminal/owned-session records, bounded diagnostics and build/install logs. Result SHA256 is `f3e6817785840bbbf5adb6f4489a344aa1f90cddc6df9cb3c349cd7066235e4c`. Preparation records describe the pre-run state; terminal/result records establish the actual execution. No binaries, full SDK inventory, environment dump or business data files are archived.
