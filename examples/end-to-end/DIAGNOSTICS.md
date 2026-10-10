# Select and inspect diagnostics

Diagnostics observe an invocation; source permissions, publication guards and
acknowledgements remain owned by the query workflow. Use the selected
`ashlar-host-otel-http/0.1` profile with Python 3.11 on POSIX and the pinned
`diagnostics` extra. The default invocation does not discover or construct an
OpenTelemetry SDK.

Install a reviewed Ashlar wheel with its diagnostics extra into the selected
query environment. Native query dependencies and the trusted Weft installation
remain separate prerequisites; follow the query guide for those resources.

## Configure one invocation

Create an absolute, regular, nonsymlink JSON file with all eight fields below.
Replace `capture_root` with an absolute directory you own and `endpoint` with
your collector's OTLP HTTP base URL. This example selects a local test collector;
it must already accept `/v1/logs`, `/v1/traces` and `/v1/metrics`.

```json
{
  "profile": "ashlar-host-otel-http/0.1",
  "capture_root": "/absolute/path/to/diagnostic-captures",
  "endpoint": "http://127.0.0.1:4318/",
  "headers": [],
  "tls": "loopback-test",
  "ca_file": null,
  "environment": "test",
  "limits": {
    "max_event_bytes": 1024,
    "max_queue_records": 16,
    "max_queue_bytes": 4096,
    "max_segment_bytes": 4096,
    "max_capture_bytes": 16384,
    "max_emissions": 100,
    "max_attempts": 8,
    "export_timeout_ms": 100,
    "shutdown_timeout_ms": 1000,
    "retention_seconds": 3600
  }
}
```

For a remote collector, select HTTPS and `tls: "system"`, or `tls: "custom-ca"`
with an absolute CA file. Headers are pairs of header name and value. Keep real
credentials in the operator-owned file; do not commit them. The file supplies
explicit settings and their origins; environment variables do not override it.

Append `--diagnostics-config /absolute/path/to/diagnostics.json` to the existing
`ashlar query-commerce-paths` command. This option currently applies to that
command. Invalid settings or incompatible selected dependencies refuse the
invocation before business effects. Ordinary exporter failure can leave a local
capture with unknown remote delivery; it does not authorize a publication.

## Read a closed capture

Each invocation creates a run directory beneath `capture_root`. Select that
specific directory after the invocation closes:

```sh
ashlar diagnostics --run-directory /absolute/path/to/diagnostic-captures/RUN_ID --limit 50
ashlar diagnostics --run-directory /absolute/path/to/diagnostic-captures/RUN_ID --min-severity 17 --limit 10
```

The command emits one bounded JSON snapshot. Optional `--attempt-id` and
`--event-name` filters combine with severity and limit. `capture_complete`
describes local capture completeness; it does not mean the operation succeeded
or the collector received every signal. Check the operation outcome, loss and
unknown-delivery fields separately. An open, expired or invalid capture is
refused with a nonzero exit status.

The selected profile, event catalog, bounds and failure rules are defined in
[CONTRACT-006](../../docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md).
