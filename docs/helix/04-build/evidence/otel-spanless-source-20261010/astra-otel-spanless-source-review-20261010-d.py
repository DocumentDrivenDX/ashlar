from pathlib import Path
import json, hashlib
ROOT = Path('/Users/erik/Projects/ashlar')
OUT = Path('/private/tmp/astra-otel-spanless-source-review-20261010-d.json')
def descriptor(path):
    path=Path(path); raw=path.read_bytes()
    return {'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
def check(item):
    actual=descriptor(item['path'])
    assert actual['bytes']==item['bytes'] and actual['sha256']==item['sha256'], item['path']
    return actual
def read(path,sha):
    item=descriptor(path); assert item['sha256']==sha
    return json.loads(Path(path).read_bytes()), item
owner, owner_ref=read('/private/tmp/ashlar-spanless-source-controls-20261010-d.json','49e262c4d438b37041591c302bdc4868597f4b86c7ff631e647d97f9f0cdd10c')
worker, worker_ref=read('/private/tmp/astra-spanless-worker-review-20261010-d.json','0871b0c2c4340a783fc19c99eef95e7cb4b5b6596b217eaf6906815f274c5a96')
facade, facade_ref=read('/private/tmp/astra-otel-spanless-facade-review-20261010-a.json','abf5fdfd4deef5a612d2949b56238cf66b2fe15ecea0067932668d4877ad99d3')
gov, gov_ref=read('/private/tmp/astra-otel-spanless-governance-review-20261010-a.json','6b93196814e4855ca1b0975715d38191e0771145e70622c5530e93ad8eaaa7f4')
source=[]
for item in owner['files']:
    source.append(check({**item,'path':str(ROOT/item['path'])}))
for item in worker['sourcePins']+worker['sourceSnapshots']+[worker['script']]+worker['predecessors']:
    check(item)
for item in facade['exactFiles']:
    if Path(item['path']).name in ('otel.py','test_otel_supervision.py'):
        check(item)
check(facade['script'])
documents=[check(item) for item in gov['exactFiles']]
logs=[check(item) for item in owner['logs']]
assert worker['verdict']=='approved-source-only'
assert worker['failedCheckCount']==0 and worker['independentCheckCount']==893
assert worker['openingClosingEqual'] is True and not worker['findings']
assert worker['ownerTests']=={'run':25,'skipped':0,'failures':0,'errors':0}
assert owner['result']['tests']==39 and owner['result']['exitCode']==0
before=Path('/private/tmp/ashlar-spanless-worker-predecessor-c-20261010.py').read_bytes()
after=(ROOT/'src/ashlar_host/_otel_worker.py').read_bytes()
assert after==before.replace(b'finish(primary, (() if actual is not None else (lambda: detach(token),)))',b'finish(primary, (() if actual is not None else (lambda: detach(token),)), diagnostic_only=True)')
predecessor_snapshots=[]
for name,size,sha in [('b',33706,'7044fe68b2cdf193fb446af3d483adf109912ae204b472f1358bbeea11c1a91f'),('c',34149,'ec6f21728cdf4a732a0b8fa65b79f18f93467836d8ca3fa4e51b10b1e5103181')]:
    predecessor_snapshots.append(check({'path':'/private/tmp/ashlar-spanless-worker-predecessor-'+name+'-20261010.py','bytes':size,'sha256':sha}))
result={
 'format':'ashlar-spanless-independent-source-review/0.1',
 'verdict':'approved-exact-source-and-governance-slice',
 'scope':'Four frozen source/test files plus three already reviewed desired-state documents. Source component and finite subprocess supervision evidence only. Actual installed spanless receiver qualification remains separate.',
 'exactSourceFiles':source,
 'exactGoverningDocuments':documents,
 'approvals':{'governance':gov_ref,'unchangedFacade':facade_ref,'correctedWorker':worker_ref},
 'ownerSourceControls':owner_ref,
 'ownerLogs':logs,
 'independentControls':{
   'facade':{'groups':9,'unchangedSourceAndTestHashes':True,'coverage':facade['independentControls'],'script':facade['script'],'supervisionRegressionLog':descriptor('/private/tmp/astra-otel-spanless-supervision-regression-20261010-a.log'),'ownerSupervisionTests':14},
   'worker':{'assertions':893,'failed':0,'ownerTests':worker['ownerTests'],'finiteProjection':worker['finiteProjection'],'restorationCombinations':25,'script':worker['script'],'log':descriptor('/private/tmp/astra-spanless-worker-review-20261010-d.log'),'snapshots':worker['sourceSnapshots']},
   'combinedOwnerTests':39,
   'parentFinalChecks':['All four source/test pins, three governing document pins, referenced receipts/scripts/logs and worker snapshots match.','Facade and supervision bytes are unchanged from their independent approval.','C to D production change is exactly diagnostic_only=True on the new SDK context-restoration finish call.','Owner added meaningful opposite-direction cancellation controls without removing prior test definitions.']
 },
 'resolvedFindings':[
   {'id':'SW-001','counterexample':worker['predecessors'][0],'resolution':'During spanless LogRecord construction, attach a genuinely empty public SDK Context and restore it before emit. No post-construction field repair, invented span or traced-event stripping. All sixty untraced records remain absent in real SDK and encoded OTLP despite an attached valid ambient context; sixty traced records preserve their contexts.','reachabilityLimit':'The predecessor isolated worker did not establish valid ambient context itself; the counterexample did not prove a facade-reachable leak.'},
   {'id':'SW-002','counterexample':worker['predecessors'][1],'resolution':'The new diagnostic-only cleanup call now preserves the first non-Exception cancellation over ordinary construction failure while retaining an earlier cancellation. All twenty-five construction/restoration combinations pass and restore context.'}
 ],
 'preservedPredecessorSource':predecessor_snapshots,
 'runtime':worker['selectedRuntime'],
 'retainedHistoricalQualification':descriptor('/private/tmp/ashlar-spanless-source-qualification-20261010-a/qualification.json'),
 'historicalQualificationScope':'The retained root 35-test receipt describes predecessor B, portable unchanged config/capture controls and selected 698 SDK payload custody. It is not represented as the final D test run.',
 'helixCorrespondence':{
   'modularity':'Public facade owns explicit invocation selection and ContextVar binding; private worker owns SDK projection. Existing process and transport ownership is unchanged.',
   'configuration':'Exact bool with True default; False refuses explicit parent/retry before effects. No profile, configuration schema, ambient setting or operator-trust discovery was introduced.',
   'observability':'Actual absence is preserved through the public SDK context mechanism. Logs and finished counters remain unchanged, and an unfinished spanless attempt alone creates no unknown span loss.',
   'formal':'Finite counterexamples and implementation correspondence checks discharge this source delta only. DIAG-F1/F2/F3/L1 remain precise specifications, not completed mechanical or production proofs.'
 },
 'findings':[],
 'limitations':['SDK metadata is an explicit application-version fixture in component tests, not an installed application observation.','Async/thread tests establish binding isolation, not concurrent delivery throughput or broad asynchronous runtime qualification.','No network, receiver, native engine, publication, ACK, installation or full C006 support claim is made by this review.','No re-audit of all SDK/OS dependencies or broad performance, outage, privacy-sentinel or investigator-pilot qualification.'],
 'reviewScript':descriptor(__file__)
}
with OUT.open('x') as f: json.dump(result,f,indent=2); f.write('\n')
print(json.dumps(descriptor(OUT)))
