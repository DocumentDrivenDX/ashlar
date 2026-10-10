---
ddx:
  id: CONTRACT-006
  type: contract
  activity: design
  status: draft
  authoring:
    home: repo
  links:
  - id: ADR-001
    kind: informed_by
---

# Contract: host diagnostics

**Type:** event, schema and host boundary. **Version:** ashlar.diagnostics/0.1.
**Related:** [ADR-001](../adr/ADR-001-delta-canonical-and-serving-layout.md),
[TD-001](../technical-designs/TD-001-publication-recovery.md),
[TP-001](../../03-test/consumer-conformance-plan.md#module-configuration-and-diagnostic-verification).

## Purpose

Operators must be able to identify the failed operation and phase, distinguish a
refusal from an uncertain effect, and determine which diagnostic evidence is
missing. Diagnostics MUST preserve the publication, source ACK and held-read
authority defined by CONTRACT-001/004. A diagnostic record is an observation,
never an authorization, checkpoint, commit receipt or result-release gate.

## Scope and boundaries

The installed host owns sanitized events, local capture and an explicitly
selected OpenTelemetry (OTel) integration. OTel is the OpenTelemetry project's
model and transport for logs, traces and metrics. The portable `ashlar` library
MUST remain dependency-free and MUST NOT discover SDKs, endpoints or credentials.

`ashlar_host.config` owns immutable typed `DiagnosticsConfig` values;
`ashlar_host.diagnostics` owns validation, event identity, capture and retrieval;
`ashlar_host.otel` owns SDK construction, signal mapping and bounded export.
Composition injects these boundaries into host workflows. The diagnostics owner
MUST NOT import publication/native SDKs or obtain a publication/ACK mutation port.
The OTel owner MUST NOT import checkout tools or rewrite compiler/native output.

This version selects `ashlar-host-otel-http/0.1`: OTel specification **1.61.0**,
resource semantic conventions **1.44.0**, and Python API/SDK/OTLP HTTP exporter
**1.45.1** (semantic-conventions package **0.66b1**). These are distinct version
namespaces. The integration uses direct SDK emission and OpenTelemetry Protocol
(OTLP) HTTP Protobuf for logs, spans and metrics. See the pinned
[logs model](https://github.com/open-telemetry/opentelemetry-specification/blob/v1.61.0/specification/logs/data-model.md),
[resource conventions](https://github.com/open-telemetry/semantic-conventions/blob/v1.44.0/docs/resource/README.md),
[Python release](https://github.com/open-telemetry/opentelemetry-python/releases/tag/v1.45.1)
and [export rules](https://github.com/open-telemetry/opentelemetry-specification/blob/v1.61.0/specification/protocol/exporter.md).
Receiver evidence qualifies the selected integration; schema validation alone
does not. JSON Lines (JSONL) capture is a local projection, not OTLP.

## Normative surface

The closed Draft 2020-12 schemas are
[diagnostic-event-v0.1](schemas/diagnostic-event-v0.1.schema.json) and
[diagnostic-run-v0.1](schemas/diagnostic-run-v0.1.schema.json). Unknown members,
duplicate JSON members, invalid UTF-8, nonfinite numbers and unknown versions
MUST refuse. Readers MUST bound bytes before decoding, nesting to 12 levels and
integer tokens to 20 digits. Schema lengths count characters; byte, provenance
and cross-record checks below remain mandatory semantic checks.

### Configuration and ownership

An explicit `None` configuration disables this profile without SDK discovery.
Selection requires a typed configuration before clients or operation effects.
There are no endpoint, credential, capture-root or deployment-environment
defaults. The host receives an absolute private capture root, one absolute
OTLP HTTP(S) base endpoint with no userinfo/query/fragment or existing signal
suffix, secret-typed headers, TLS policy, deployment environment and the limits below. A loopback HTTP endpoint
is permitted only by an explicit test policy; other endpoints require verified
HTTPS. Remove one trailing slash and append `/v1/logs`, `/v1/traces` and
`/v1/metrics` once to the selected base.
Never print the endpoint, headers, secret values or their hashes.

`DiagnosticsConfig` has exactly `profile`, `capture_root: Path`,
`endpoint: SecretText`, `headers: tuple[(str, SecretText), ...]`,
`tls: Literal["system", "custom-ca", "loopback-test"]`, `ca_file: Path | None`,
`environment`, `limits` and `origins`. `SecretText` is a host-owned immutable
secret wrapper with redacted string/repr and explicit private reveal; it cannot
be serialized as settings. Headers have at most eight unique case-insensitive
names (ASCII HTTP token, ≤64 bytes) and values (≤2048 UTF-8 bytes, no control
characters). `custom-ca` requires one absolute regular CA file ≤1 MiB; other
policies require `ca_file=None`. `origins` maps every configuration leaf except
itself (limit names use the `limits.` prefix) to one of `explicit`, `environment-secret`,
`development-env`, `environment-toml`, `default-toml`, `field-default`. Required
operator settings cannot originate in either TOML or field defaults. Unknown
fields, booleans in integer settings and invalid combinations refuse. Persist
only this origin map as `configuration_origins`; endpoint, headers, CA path and
capture-root values are never included in the run manifest.

Configuration follows ADR-001 precedence and origin tracking at composition;
the adapter receives immutable values and MUST ignore ambient `OTEL_*`, proxy,
credential and resource-detector settings. Do not attach to global providers,
auto-instrument, ingest JSONL with a second exporter or enable exporter debug
payloads. Each run owns one provider set and one route per signal. Missing SDKs,
invalid settings or an unsupported interpreter refuse the selected profile before
operation effects; Python 3.9 portable-core support is unchanged.

| Limit | Allowed configuration and meaning |
| --- | --- |
| `max_event_bytes` | 512–4096 UTF-8 bytes, including the JSONL newline; reject whole oversize events |
| `max_queue_records`, `max_queue_bytes` | 1–128 and 4096–524288, separately for logs and spans; reserve before enqueue, drop newest without waiting |
| `max_segment_bytes`, `max_capture_bytes` | 4096–4194304 and 4096–16777216; at most four segments; segment ≤ capture |
| `max_emissions`, `max_attempts` | 1–10000 encodable sequence positions and 1–64 admitted invocation identities per run; later emissions are counted as dropped |
| `export_timeout_ms`, `shutdown_timeout_ms` | 1–1000 and 1–2000, measured with a monotonic clock; one total shutdown deadline, not one per signal |
| `retention_seconds` | 1–604800 after closure; no automatic deletion outside the run's owned directory |

The maximum event MUST fit both queue and segment limits. Logs and spans each
use their selected queue record/byte budgets, including processor-owned pending
and in-flight records; retries reuse that reservation. An SDK's record-count
limit alone does not establish a byte bound. Metric aggregation has at most 20
operation/outcome series; its accumulator and at most one pending or in-flight
snapshot together MUST use ≤65536 retained payload bytes. A new snapshot while
that slot is occupied is dropped and counted without resetting the accumulator.
These limits exclude SDK object/runtime overhead, which requires separate
measurement; they are not process-memory guarantees.

Each signal has at most one serialized request in flight, ≤1048576 bytes before
compression. Each HTTP response body, both encoded and decoded, MUST be ≤65536
bytes. These transport buffers are separate from the queue and metric-state
budgets. Retries share the original deadline and cannot create an extra payload
reservation or concurrent request. No background worker or owned child may
survive successful diagnostic closure. A blocked transport cannot extend the
host deadline.

### Event catalog and identity

Only the schema's fixed bodies, attribute names and enums may reach any sink.
The host assigns random opaque `run_id` and per-invocation `attempt_id` values;
they MUST NOT encode source, user, path, query or credential material. A retry
receives a new attempt ID. The sole local emitter is `host`; a run cannot be
shared across processes. Concurrent attempts in that process share one sequence
allocator. Each attempted emission consumes the next sequence number, including
a dropped event; gaps disclose loss and MUST NOT be repaired by renumbering.

| Event | Body | Severity and additional attributes |
| --- | --- | --- |
| `ashlar.operation.started` | `Operation started` | INFO/9; run, attempt, emitter, sequence and operation |
| `ashlar.operation.phase` | `Operation phase observed` | INFO/9; base attributes plus phase and phase state |
| `ashlar.operation.finished` | `Operation finished` | `succeeded`: INFO/9; `refused`, `cancelled`, `uncertain`: WARN/13; `failed`: ERROR/17; outcome required; safe error category required except on success |

Operations are `publication`, `source-admission`, `held-read` and
`ack-reconciliation`. Phase observations MUST come from the owning transition or
guard; a successful telemetry write cannot create a completed phase. The owner
emits exactly one finished event attempt per invocation, after its operation's
required cleanup and closing guards. Unknown native effects remain `uncertain`;
catching an exception alone cannot classify them as rolled back. A cleanup-only
operation failure is `failed` with category `cleanup`. An original cancellation
remains `cancelled` even when cleanup also fails. `cleanup_failed` records that
additional fact. Failure is recorded once at this owning boundary.

### OTel mapping

| Local field | Exact OTel destination |
| --- | --- |
| `timestamp_unix_nano` | LogRecord Timestamp as an unsigned 64-bit nanosecond integer; omit if unknown |
| `observed_timestamp_unix_nano` | LogRecord ObservedTimestamp captured when the host observes the event; never substitute an invented source time |
| `severity_text`, `severity_number`, `body`, `event_name` | Corresponding LogRecord fields; Body is the catalog string and EventName is not an attribute fallback |
| `resource` | Exactly the schema's six Resource attributes; suppress extra process/host/environment detectors |
| `scope` | InstrumentationScope name `ashlar.host.diagnostics`, version `0.1.0`; scope schema URL omitted |
| `attributes` | Same keys and primitive types, plus `ashlar.diagnostic.schema_version` from `schema_version` |
| optional `trace` | Decode lowercase hex to TraceId/SpanId; copy the actual 8-bit TraceFlags without truncation or synthesis |

Resource schema URL is `https://opentelemetry.io/schemas/1.44.0`. `service.version`
is the installed Ashlar distribution version; the SDK identity is independently
`opentelemetry`/`python`/`1.45.1`. Only the four schema-listed environment names
are admitted. Run/attempt IDs are log/span attributes, never Resource values,
metric dimensions or backend stream labels.

Trace context is all-or-none and must be a valid actual SDK span context, with
nonzero 16-byte trace and 8-byte span IDs. No active valid span means no `trace`
field and unset OTLP trace fields. Run/attempt IDs MUST NOT manufacture context.
Each operation span is `ashlar.<operation>`, kind INTERNAL; an explicitly passed
valid context supplies its parent. An independently retried operation starts a
new root with at most one link to the original valid span context. Do not copy
baggage or arbitrary tracestate to diagnostic attributes. The profile uses
AlwaysOn sampling for newly created spans; it neither samples local/log events
nor rewrites an observed context's flags. Use the same Resource/scope as logs.
Span attributes are run ID, attempt ID and operation, followed at end by outcome
and cleanup_failed; safe error category appears only for a nonsuccess outcome.
Status is UNSET on success and ERROR otherwise, without a description. Automatic
exception recording, raw status descriptions and duplicate span events are
forbidden. The pinned [trace API](https://github.com/open-telemetry/opentelemetry-specification/blob/v1.61.0/specification/trace/api.md)
owns span/context representation.

The synchronous Counter `ashlar.operation.completed`, unit `{operation}`, adds
one per finished invocation, with only `ashlar.operation` and `ashlar.outcome`
dimensions. Export it as a cumulative monotonic integer Sum through the same
Resource/scope and endpoint family. This counts observed invocations, not
published rows, committed transactions or ACKs. Exemplars are disabled in this
profile. The pinned [metrics API](https://github.com/open-telemetry/opentelemetry-specification/blob/v1.61.0/specification/metrics/api.md)
owns Counter semantics. Export success is a handoff observation, never proof of
durable receiver ingestion or exactly-once delivery after retry ambiguity.

### Privacy, capture and retrieval

All three projections receive the same validated immutable event. Construction
MUST reject free-form messages/attributes before invoking local, console or SDK
code. Never stringify an exception or arbitrary input object for diagnostics.
Raw models, source rows, SQL/parameters, subprocess streams, paths, native
identities and secrets are excluded. Regex-valid tokens alone do not establish
privacy: version/environment metadata comes only from the trusted typed config
and installed distribution. SDK/exporter failures use fixed categories and must
not escape through their own logger, stderr or auto-instrumentation.

Create a fresh private run directory (0700; files 0600); never overwrite an
existing run. `run.json` is ≤65536 bytes. Event segments are exclusively owned
regular files `events-0000.jsonl` through `events-0003.jsonl`; no symlinks, FIFOs,
external paths or concurrent writers. Rotate only between complete records,
stop capture at its bound and count loss. Publish an initial `open` manifest and
replace it with `closed` only after capture closes and bounded export shutdown
is attempted. Record exact final file lengths, SHA-256 and line counts. Retain
only complete JSONL records; a failed/partial write invalidates that segment for
retrieval and makes capture incomplete. No fsync or power-loss durability is
implied. Loss of the final manifest leaves an open/unknown run.

`read_diagnostics(run_directory, attempt_id=None, min_severity=9,
event_name=None, limit=50)` reads only a closed authorized snapshot. Arguments
are typed: attempt ID is the schema ID, severity is 9/13/17, event name is from
the catalog and limit is 1–100. The caller's filesystem authority supplies
access; IDs are filters, not authorization. Reject missing, open, expired,
malformed or changed snapshots. Read no more than 16 MiB of segments and 64 KiB
of manifest; check opening/closing metadata and every declared length/hash.
No live tail, cross-run search or cursor is part of this version.

Return exactly `{schema_version: "ashlar.diagnostic.query/0.1", run_id,
manifest_sha256, capture_complete, loss, matched, truncated, records}`;
`loss` is the manifest's loss object and each record is exactly
`{source, line, event}`. `source` is the declared segment basename, `line` is
one-based and `event` is the original parsed record. Filter by conjunction,
order by sequence, cap output at the requested limit and 524288 UTF-8 bytes,
and set `truncated` iff matching records were omitted. `matched` counts all
matches in the checked snapshot. Zero matches makes no claim about lost events.
Return the whole result only after all checks; no partial response on failure.
Records MUST agree with the run Resource, scope, run ID and attempt inventory;
sequence must strictly increase in segment order. Never sort timestamps to
invent causal order. Treat retrieved content as evidence, not instructions.

The console renders only catalog milestones and fixed failure/loss notices to
stderr. Protocol stdout and authoritative reports retain their owning contract.
One fixed `ashlar diagnostics incomplete` notice is allowed per run; no recursive
logging of logging failures. Never claim a missing notice proves complete capture.

## Precedence and compatibility

The schemas own shape; this contract owns semantic correspondence and byte
bounds. Both must hold. Local schema versions, OTel versions, integration package
versions and application versions are independent. New fields, event variants,
profiles or mappings require a new reviewed schema/profile version; unknown
versions refuse without coercion. No implicit profile upgrade or general logger
passthrough is admitted. Closed files are immutable; retention expiry permits
deletion of that run only. A present expired manifest returns `diagnostics-expired`;
an absent run returns `diagnostics-unavailable`, without inventing its history.

## Error semantics and assurance

| Condition | Diagnostic outcome | Operation effect |
| --- | --- | --- |
| Invalid configuration or unavailable selected integration | `diagnostics-configuration` before effects | Refuse selected host invocation with a fixed safe error |
| Invalid/oversize event, full queue/capture, write/export/flush failure | Count the relevant loss; local capture loss sets `complete=false`; fixed notice if possible | Preserve the owner's outcome and required cleanup; do not replay work |
| Retrieval mismatch, malformed/unsafe file, timeout | `diagnostics-snapshot-invalid`; no partial result | No native/publication/ACK calls |
| Open/missing/expired snapshot | `diagnostics-open`, `diagnostics-unavailable` or `diagnostics-expired` | No fabricated empty success |
| Primary failure plus diagnostic cleanup failure | Preserve the original exception object, including any non-Exception cancellation | Mark bounded loss without replacing, suppressing or stringifying the primary |

If cancellation first occurs during diagnostic shutdown, propagate that original
non-Exception cancellation after bounded owned cleanup. Ordinary diagnostic-only
shutdown failures do not change a completed operation's classification. A
diagnostic loss flag cannot stand in for required native cleanup or closing checks.

Counters are exact nonnegative integers while known; use `null` when unavailable
or overflowing, never zero as a substitute. `local_attempted = local_written +
local_dropped` when all are known. Export counters use the following logical
units, independently for `loss.logs`, `loss.spans` and `loss.metrics`:

| Signal | One accounting unit |
| --- | --- |
| Logs | One admitted immutable LogRecord offered to the export path |
| Spans | One ended immutable Span offered to the export path |
| Metrics | One cumulative integer Sum NumberDataPoint for one operation/outcome series at one collection instant; neither a Counter measurement nor a request/batch |

`submitted` increments once when a unit is offered, including a unit then rejected
by capacity. An invalid event refused before export admission is local loss only.
`handed_off` counts each logical unit at most once after an observed full-success
OTLP response. `dropped` counts a unit discarded before any possible transmission.
Retries retain the same unit identity and MUST NOT increment any of these
counters again. A new metrics collection creates new data-point units even when
their cumulative values contain previously observed measurements; dropping a
point does not subtract measurements from the Counter. For example, cumulative
values 1 then 3 for one series are two submitted data points, not four operations;
retrying either batch does not add a third submitted point.

If an outcome is ambiguous (timeout after possible transmission, partial-success
response, or SDK accounting without unit-level knowledge), set the affected
count to `null` and `unknown=true`; never infer zero loss from a successful SDK
flush. With exact counts, `handed_off + dropped ≤ submitted`; at closed drain
with `unknown=false`, equality MUST hold. `flush` describes the local attempt:
`complete` means successful processor drain and no pending owned units;
`incomplete` means deadline exhaustion; `failed` means an observed cleanup/flush
failure; `not_attempted` means no flush was attempted. It is independent of remote
delivery, which may remain unknown even after local drain. Receiver tests MUST
compare logical units separately from HTTP request/retry counts and metric values.
The run's `complete` means local capture completeness only: closed valid segments,
all attempted events captured, no unknown local counts. It does not mean the
business operation succeeded or remote telemetry is complete. Attempt IDs and
segment names MUST be unique independently of other fields. `local_written`
equals the sum of declared segment records; exclude every record of an invalid
segment and count that loss. A null counter requires its corresponding unknown
flag. Encoded sequences, admitted attempts and files must fit the selected limits,
including abandoned partial bytes. Total attempted/dropped counters can exceed
the emission limit. Expiry equals close time plus retention, within unsigned
64-bit range.

**DIAG-F1 (authority isolation):** diagnostic transitions leave TD-001's source,
publication vector, attempt phase and per-feed ACK state unchanged. PUB-F1/F2
release/progress guards remain required even if every diagnostic sink succeeds.
**DIAG-F2 (privacy/bounds):** every sink sees only admitted values within the
declared budgets. **DIAG-F3 (failure preservation):** diagnostics cannot replace
a primary failure or classify an uncertain native effect as success.
**DIAG-L1:** each diagnostic emission is nonblocking at capacity and close returns
within its total deadline, assuming the reviewed host clock/transport and
cancellable owned-resource operations. Diagnostic liveness does not strengthen
PUB-L1's native availability/fair-retry assumptions.

Assurance is precise specification plus implementation correspondence, not a
mechanical proof. TP-001 controls must include real SDK logger → OTLP receiver
mapping for one traced event and one untraced event, the matching span and
counter; exact identities/types/times, no duplicate ingestion, concurrent
attempts/retry links, sentinel privacy across every sink, queue/record/capture
loss, exporter outage and cleanup cancellation. A finite model may treat all
diagnostic actions as stuttering over publication state; it must separately
model loss and failure preservation, and state its bounds/assumptions. It cannot
prove engine semantics, SDK memory bounds, filesystem durability or receiver
delivery. Small one-shot overhead budgets and actual receiver commands belong
in the reviewed test evidence before an integration claim.

## Examples

An untraced event has no `trace` member:

```json
{"schema_version":"ashlar.diagnostic.event/0.1","observed_timestamp_unix_nano":"1791590400000000000","severity_text":"INFO","severity_number":9,"event_name":"ashlar.operation.started","body":"Operation started","resource":{"service.name":"ashlar-host","service.version":"0.1.0.dev0","deployment.environment.name":"test","telemetry.sdk.name":"opentelemetry","telemetry.sdk.language":"python","telemetry.sdk.version":"1.45.1"},"scope":{"name":"ashlar.host.diagnostics","version":"0.1.0"},"attributes":{"ashlar.run.id":"11111111111111111111111111111111","ashlar.attempt.id":"22222222222222222222222222222222","ashlar.emitter.id":"host","ashlar.sequence":1,"ashlar.operation":"held-read"}}
```

For the traced case, the same shape additionally contains
`"trace":{"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","trace_flags":1}`
only when those are the actual active SDK context. These illustrative identifiers
are not IDs to install or manufacture in an implementation.
