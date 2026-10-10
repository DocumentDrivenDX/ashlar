from pathlib import Path
import hashlib,json
F=Path('/private/tmp/ashlar-otel-worker-source-freeze-20261010-d')
R=Path('/Users/erik/Projects/ashlar')
def pin(p):
 p=Path(p);b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
m=json.loads((F/'manifest.json').read_text());assert pin(F/'manifest.json')['sha256']=='26823f128a63d31c1acd1f4e472f552625f911332c4508c5e54c25a8e282b9f2'
for d in m['files']:
 for p in (F/d['path'],R/d['path']):
  a=pin(p);assert a['sha256']==d['sha256'] and a['bytes']==d['bytes']
controls=json.loads(Path('/private/tmp/astra-otel-worker-cleanup-controls-20261010-d.json').read_text());assert controls['ownerTests']==18 and all(x['cancelIdentity'] for x in controls['checks'])
projection=pin('/private/tmp/astra-otel-worker-projection-review-20261010-c.json');assert projection['sha256']=='edd61446bf75ae78b722be33b50b2fbb65a74fc6b4a3b9223f144f4d132c0038'
v={'verdict':'approved-source-only','scope':'Exactly frozen worker D and focused tests; no installation, actual HTTP/receiver, host/native/ACK or complete C006 qualification. Parent facade reviewed separately.',
'freeze':pin(F/'manifest.json'),'sourceFiles':[pin(F/d['path']) for d in m['files']],'liveEqualsFrozen':True,
'correctionDisposition':{
'WT-001':'Closed logs/spans/metrics route map now emits /v1/logs,/v1/traces,/v1/metrics while preserving base prefix; meaningful inert transport controls pass.',
'WP-001':'Resource schema URL and exact six attributes/scopes; diagnostic schema log attr; unknown source Timestamp remains omitted; 428 independent finite SDK projection assertions pass on C projection regions proven byte-identical in D.',
'WP-002':'ashlar.<operation> span names and exact admitted run/attempt/operation/outcome/cleanup/error attrs and status; success/failure cases independently rechecked.',
'WT-002':'D distinguishes diagnostics-only accumulators from explicit owner primary; original six SDK collect/HTTP failure-followed-by-cancellation cases now preserve exact KI/SystemExit/GeneratorExit; owner primary default remains preserved.',
'WT-003':'Worker emits bounded private begin before transport construction/I/O and complete after successful owned cleanup; failed cleanup permanently refuses later sends and cannot reset parent deadline. Actual hard deadline depends on matching parent supervision; worker direct use/socket timeout alone is not qualified.'},
'actualVerification':{'parentCommand':'PYTHONPATH=/Users/erik/Projects/ashlar/src /private/tmp/ashlar-otel-sdk-env-20261010-a/bin/python -B -W error::ResourceWarning /private/tmp/astra-otel-worker-cleanup-controls-20261010-d.py','ownerTests':18,'parentIndependentReproControls':6,'childIndependentProjectionAssertions':428,
'artifacts':[pin('/private/tmp/astra-otel-worker-cleanup-controls-20261010-d'+s) for s in ('.py','.json','.log')]+[pin('/private/tmp/astra-otel-worker-projection-review-20261010-c'+s) for s in ('.py','.json','.log')]},
'governing':[pin(R/'docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md')],
'helix':{'modularity':'Private SDK/queue/transport owner with sanitized event ports; no native/publication/ACK interfaces or global provider attachment. Parent owns environment and process lifetime.','configuration':'Explicit frozen settings, closed schemas and bounded integer limits; ambient SDK/proxy knobs refused before providers; package version admission remains actual-installed integration work.','observability':'Exact low8 SDK context flags projected with native upper bits preserved; logical-unit counts and bounded local uncertainty independent of remote ingestion. No runtime/receiver proof from fake ports.','formal':'Precise DIAG state/cleanup and projection correspondence plus finite counterexample/regression controls; no analyzer/model proof or production liveness guarantee.'},
'separateRemainingIntegrationObligations':['Match private progress protocol with independently reviewed parent facade; exercise actual isolated blocked I/O and deadline/reap behavior.','Actual installed parent/worker and real receiver/protobuf output closure, failure/privacy/concurrency/retry controls and pilot gates remain.','Contract-valid no-active-context behavior has not been established as reachable through current required per-operation span path; do not claim the untraced receiver control from this source slice.','Payload budgets exclude SDK object/process memory.'],
'priorEvidence':[pin('/private/tmp/astra-otel-worker-source-review-20261010-a.json'),pin('/private/tmp/astra-otel-worker-cleanup-controls-20261010-c.json')]}
p=Path('/private/tmp/astra-otel-worker-source-review-20261010-d.json');p.write_text(json.dumps(v,indent=2)+'\n');print(json.dumps(pin(p)))
