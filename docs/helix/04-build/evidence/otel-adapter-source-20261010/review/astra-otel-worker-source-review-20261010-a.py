from pathlib import Path
import json,hashlib
F=Path('/private/tmp/ashlar-otel-worker-source-freeze-20261010-a')
R=Path('/Users/erik/Projects/ashlar')
def pin(p):
 p=Path(p);b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
m=json.loads((F/'manifest.json').read_text())
for d in m['files']:
 actual=pin(F/d['path']);assert actual['bytes']==d['bytes'] and actual['sha256']==d['sha256']
controls=json.loads(Path('/private/tmp/astra-otel-worker-transport-controls-20261010-a.json').read_text())
child=json.loads(Path('/private/tmp/astra-otel-worker-projection-review-20261010-a.json').read_text())
receipt={
 'verdict':'changes-required-source-only',
 'scope':'Independent review of frozen private SDK worker A and focused tests against C006/latest ADR. No implementation edits, Git mutation, receiver/network, credential reading, native or compiler execution.',
 'freeze':pin(F/'manifest.json'),'files':[pin(F/x['path']) for x in m['files']],
 'governing':[pin(R/'docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md'),pin(R/'docs/helix/02-design/adr/ADR-001-delta-canonical-and-serving-layout.md')],
 'findings':[
 {'id':'WT-001','priority':1,'lines':[312], 'issue':'Internal spans signal generates /v1/spans instead of OTLP /v1/traces.', 'evidence':'All three HTTP paths observed through inert HTTPConnection; logs/metrics correct, spans wrong.', 'correction':'Closed internal signal to endpoint mapping, including prefixed base endpoint controls.', 'successor':'Owner reports B fixes route; B not approved by this A receipt.'},
 *[dict(x,priority=1) for x in child['findings']],
 {'id':'WT-002','priority':1,'lines':[62,529], 'issue':'Cleanup-only ordinary error before first non-Exception cancellation suppresses that cancellation.', 'evidence':'finish(None, [OSError, cancellation, final-cleanup]) raises OSError for each KeyboardInterrupt/SystemExit/GeneratorExit; all cleanup actions run.', 'correction':'Retain an explicitly supplied owner primary. Without one, prioritize the first non-Exception shutdown cancellation over prior ordinary diagnostics-only cleanup failures; keep best-effort marker and remaining cleanup.'},
 {'id':'WT-003','priority':1,'lines':[302,318], 'issue':'Per-request deadline gap includes connect/write/header progress, not only documented hostname DNS. A socket timeout is an inactivity bound, not a total request bound.', 'evidence':'Real stdlib HTTPResponse parses an inert drip-header file for 0.20 synthetic monotonic seconds against a 0.10-second export budget, and only then refuses; no DNS/socket/network execution.', 'correction':'The planned parent-supervised private transport-deadline protocol can supply the wall bound, with begin-before-I/O, complete-after-cleanup, validated nonextending shared/individual deadlines and kill/reap on expiry. This is a source/integration gap until implemented and tested, not a request to weaken C006.'}
 ],
 'actualChecks':{'ownerFocusedTests':controls['ownerTests'],'parentControls':controls['controls'],'childBoundedSdkChecks':child['check_count'],
 'commands':['PYTHONPATH=/Users/erik/Projects/ashlar/src /private/tmp/ashlar-otel-sdk-env-20261010-a/bin/python -B -W error::ResourceWarning /private/tmp/astra-otel-worker-transport-controls-20261010-a.py','Child exact command retained in projection script/log receipt'],
 'artifacts':[pin('/private/tmp/astra-otel-worker-transport-controls-20261010-a'+s) for s in ('.py','.json','.log')]+[pin('/private/tmp/astra-otel-worker-projection-review-20261010-a'+s) for s in ('.py','.json','.log')]},
 'positiveScope':child['positive_controls']+['Closed bounded frame/parser inputs, explicit frozen Settings, no native/publication/ACK imports or ports, SDK providers owned privately without global attachment, ordinary protocol refusal fixed and payload-free'],
 'separateIntegrationGaps':child['capability_gaps']+['Parent otel.py/process supervision is absent in this freeze. No full C006 host integration, installed application version, actual HTTP receiver/export path, no-active-context fallback, process lifetime or deadline qualification is granted.'],
 'helixCorrespondence':{
 'modularity':'Reviewed private worker SDK/protocol/queue/transport ownership under parent facade; no native/authority boundary crossings found.',
 'configuration':'Explicit immutable wire snapshot and closed finite limits; SDK/proxy ambient settings refused. Actual parent clean environment remains an integration obligation.',
 'observability':'Exact mappings and real receiver qualification are required; inert SDK tests alone do not qualify transport or host route.',
 'formal':'DIAG-F1/F2/F3/L1 precise-only semantic/source correspondence: authority isolation held at this private boundary; mapping/deadline/cancellation counterexamples require repair and affected controls recheck. No analyzer/model or production proof claimed.'},
 'limits':['Owner source freeze unchanged and independently rehashed. Actual application distribution version mocked only for SDK fixture tests. Queue bounds are retained payload limits, not total SDK/process memory bounds. No external specification/network retrieval was performed.']}
p=Path('/private/tmp/astra-otel-worker-source-review-20261010-a.json');p.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(pin(p)))
