# One-shot configured-local `.4` initialization packet

This packet is prepared for Sol review and has not been prepared or executed. It carries the admitted four source inputs and builds only the explicitly versioned `ashlar-configured-local/0.3` initialization invocation with `carrier_profile=ashlar-delta/0.4-xxhash64`. Its only native action is one fresh local six-role Delta initialization. It does not publish, query, mutate Truss/source state, ACK, or contact cloud services.

The original failed attempt and the later metadata recheck remain unchanged in the parent evidence directory. This packet fixes the missing recoverability by retaining `packet_supervisor.py`, `packet_worker.py`, the exact launch descriptor, and a pre-Spark child preflight. The child checks its own full argv, interpreter identity, allowlisted environment and digest, Python distribution versions and RECORD hashes, Java and exact Delta JAR hashes, local wheel hashes, selected UMF producer revision/tools, source/config input bytes, current Ashlar source-tree hashes, and all six proposal-only UMF bridge results. It then repeats mutable package/source checks immediately before calling the current `initialize_configured_local` implementation. No code in packet preparation or preflight creates a Spark session.

The bounded local initialization is within the existing release authorization. The supervisor requires a permit binding the prepared invocation, launch digest, and reviewed packet digest, plus the Sol packet-review receipt named `sol-review-<packetTreeSha256>.json` and a process-slot receipt captured within 60 seconds that reports no Java/Spark process. The exact child command and allowlisted environment are frozen in `launch.json`; their canonical digests are checked inside that same child before the first Spark call. A `dispatch.json` is created exclusively before process launch, so a dispatched attempt cannot be retried. The supervisor terminates the child process group on the 300-second wall limit, 512 MiB owned-attempt limit, or 1 MiB combined stdout/stderr limit.

Focused packet checks were run with the pinned local Python and passed. They verify the exact runtime and JAR attestation, exercise the six UMF proposals without importing Spark or starting a JVM, and prove that an isolated child launched with `-I -B -u` sees those flags in `sys.orig_argv` even though they are absent from `sys.argv`. During packet self-check, the original distribution RECORD digests were found to be wheel/source-layer digests rather than this installed environment’s RECORD bytes: installed records contain installer-normalized paths and generated entries. The packet now pins the observed installed RECORD bytes while independently pinning each cached wheel SHA-256; all four installed distributions and wheel hashes are checked. The first native attempt started local Spark and failed after creating only the `object_current` Delta log. The compact immutable receipt `../failed-native-attempt-52c8424075d3435b9d3245bc0e30b943.json` preserves the supervisor result, stderr hash, first-role Delta log/schema hashes, and diagnosis: Spark `LongType.simpleString()` returns `bigint`, while UMF maps the Delta `long` type; comparing these display names caused a false refusal. The fix compares `DataType.typeName()` and a focused no-JVM regression covers StringType, LongType, and TimestampType. The failed attempt remains untouched in private temporary storage; no retry or publication occurred. A second native attempt passed schema validation but failed at first-role protocol validation because the adapter imported a nonexistent `ashlar_host.protocol` and recreated the configured reader profile incorrectly. Receipt `../failed-native-attempt-2c07637d5d514e7b97d42f80d0e83bf1.json` captures its supervisor result, stderr hash, Delta schema/protocol metadata hashes, and diagnosis. The fix imports `validate_protocol_detail` from `ashlar.protocol` and passes the existing configured-local `ReaderProtocolProfile`; a focused no-JVM check uses a realistic `DESCRIBE DETAIL` shape. The second attempt also remains untouched; no retry or publication occurred.

To inspect the source packet digest for Sol review, run `packet_digest.py`. After Sol review, the supervisor supports `prepare` to create a fresh private attempt. `execute` requires the reviewer-bound permit file and refuses stale or mismatched packet, invocation, child launch, or slot receipt. Do not prepare or execute this packet as part of review. The future attempt directory remains isolated under `/private/tmp/ashlar-delta4-init-attempts/`; preserve its compact result and captured stream hashes before deleting only that exact owned directory.

The prior packet review receipt remains unchanged at the legacy `sol-review.json` path. Each packet revision uses a distinct receipt filename derived from its lowercase 64-character tree digest, so reviews cannot be overwritten or reused across revisions. The permit JSON shape is exact:

```json
{
  "profile": "ashlar-delta4-init-execution-permit/0.1",
  "packetTreeSha256": "<reviewed packet digest>",
  "preparedSha256": "<prepared.json digest>",
  "launchSha256": "<launch descriptor digest>",
  "invocationSha256": "<ashlar-configured-local/0.3 invocation digest>",
  "processInventory": {
    "profile": "ashlar-local-process-slot-inventory/0.1",
    "capturedAt": "<UTC timestamp no more than 60 seconds old>",
    "capturedBy": "<operator>",
    "command": "ps -axo pid,comm",
    "capturedOutputSha256": "<SHA-256 of complete output>",
    "sparkOrJavaProcesses": []
  },
  "solReview": {
    "path": "<packet parent>/sol-review-<reviewed packet digest>.json",
    "sha256": "<SHA-256 of Sol review receipt>"
  }
}
```
