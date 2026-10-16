# Dedicated serverless graph layout trial

Dedicated Ashlar serverless Jobs run [458676000224358](https://adb-7405607548213398.18.azuredatabricks.net/?o=7405607548213398#job/401862977233947/run/458676000224358) succeeded with 1,000,000 nodes and 10,000,000 edges on native Spark 4.2.0. No shared cluster or SQL endpoint was used. Both tables used the canonical UMF-generated managed Delta layout, including liquid clustering by lookup_hash and declared properties. Opening and closing native identities, versions, schema digests, properties and clustering matched.

| Observation | Seconds |
| --- | ---: |
| Insert 1M nodes | 9.808 |
| Insert 10M edges | 18.500 |
| Singleton execution + fetch | 0.878 |
| Connected one-hop execution + fetch | 0.925 |
| Connected two-hop execution + fetch (400 rows) | 1.368 |
| Connected distinct-terminal execution + fetch | 1.379 |
| Notebook probe including checks | 76.298 |
| Native service run duration | 92.309 |
| Native setup duration | 4.000 |

All seven bounded independent result-bag checks passed, including the three isolate cases. Each query had one warmup and one measured sample. Validation durations are recorded separately in summary.json and the full result. Cache state was uncontrolled; these are individual observations, not latency percentiles. Direct range-generated inserts measure physical layout loading, not Ashlar publication/source/ACK throughput, Weft execution, graph-engine interoperability or production security. The fixture includes isolates, self-loops and parallel edges; it does not qualify hub skew or billion-node operation. Predictive optimization settings were not changed; effective inherited state remains unknown.

## Attempts and cost boundary

Attempt A used STANDARD startup and timed out during provisioning: native setup185s, execution0s, no notebook result, scratch schema absent. Its 180s task timeout incorrectly included startup. Commit28e7a5cf corrected task/job timeout accounting and allowed explicit performance-optimized startup. Attempt B used PERFORMANCE_OPTIMIZED, a360s task/job timeout, notebook180s cooperative budget, query90s timeout, operator cancellation at330s and no automatic retries. No third attempt was started. The saved jobs have no schedule or continuous trigger.

The approved budget was $25. The pre-run combined allowance rounds A up to200s and allows B360s. Premium centralus automated serverless retail pricing of $0.47/DBU-hour and the documented approximate default120 executor DBU/hour give $8.78 rounded up; a $15 reserve gives **$23.78**. This is a planning estimate, not a guaranteed invoice cap or actual bill. Executor limits exclude other components; billed usage is unknown. cost-plan.json and the transcribed cost-basis evidence retain these qualifications. Sources: [Azure pricing API](https://prices.azure.com/api/retail/prices), [serverless compute management](https://learn.microsoft.com/en-us/azure/databricks/compute/serverless/manage-serverless-compute), [serverless Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/run-serverless-jobs).

## Custody and cleanup

Attempt A was prepared from240c4896; B from28e7a5cf. The original and native-exported notebook sources, configurations, native job/run responses and task outputs are retained separately. manifest.json records compressed and uncompressed SHA256 for each exact archived file. gzip timestamps are zero. Operator scripts are execution receipts, not a supported general-purpose deployment API. No authentication tokens or idempotency secrets are archived.

After retaining results, UC REST captured schema/table metadata, checked current ownership, creation after run start, exact two expected managed Delta tables, native storage locations and UC IDs against the report, then rechecked identity immediately before deletion. UC IDs and Delta UUIDs are distinct. Both synthetic tables and their non-forced scratch schema were deleted and schema absence verified. No additional compute was started for cleanup. This does not assert immediate underlying storage purge. The unscheduled job/notebook records remain for inspection.

Independent review confirmed native results, scope qualifications, budget limitations and exact-resource cleanup. Local focused preparation tests passed (7 tests); native execution supplied the actual larger-layout check.
