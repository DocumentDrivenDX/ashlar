from pathlib import Path
import ast,hashlib,json
R=Path('/Users/erik/Projects/ashlar')
def pin(p):
 p=Path(p);b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def checked(p,sha):
 d=pin(p);assert d['sha256']==sha;return d
Q=Path('/private/tmp/ashlar-otel-adapter-source-20261010-b/qualification.json');q=json.loads(Q.read_text());assert q['openingSources']==q['closingSources']
for d in q['closingSources']:
 a=pin(d['path']);assert a==d
child=checked('/private/tmp/astra-otel-supervision-review-20261010-c.json','5dac9a65fee86083779899306942c10c09446a663e8bc2d56db6b34c75c7a196')
c=json.loads(Path(child['path']).read_text());assert not c.get('findings')
worker=checked('/private/tmp/astra-otel-worker-source-review-20261010-d.json','e47083dba6c81ec1c1e1c1245e8060178859f3b0c50e26d0812daca1f1874872')
closure=checked('/private/tmp/astra-otel-adapter-source-closure-20261010-b.json','eeef760b2616377506e7271bb03c042db1b9cd8e981fcfdf4d06cd8544c0d75b')
# Preserve proof that the independently exercised fixed context forwarding body remains exact.
def method(path,name):
 tree=ast.parse(Path(path).read_bytes())
 return ast.dump(next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name==name),include_attributes=False)
for name in ('operation_context','trace_context'):
 assert method('/private/tmp/astra-otel-facade-context-controls-20261010-a.source.py',name)==method(R/'src/ashlar_host/otel.py',name)
result={
'verdict':'approved-source-only',
'scope':'Eight exact source/test/configuration/ADR/CI files in root B qualification vector, including independently approved private worker D. This approval covers finite source/component controls and reviewed semantic correspondence only. No installed-host, real HTTP receiver, native/ACK or full CONTRACT-006 support claim.',
'exactFiles':q['closingSources'],'rootQualification':pin(Q),'sourceCustody':closure,'workerSourceApproval':worker,'supervisionApproval':child,
'resolvedFindings':{
'context-binding':'Parent/retry is forwarded only for started; later phase/finished use the actual stored worker attempt context. Both explicit parent and retry direct-port controls pass; affected current methods remain AST-identical to independently exercised corrected snapshot.',
'SUP-001':'Diagnostics-only ordinary cleanup/RPC failures cannot suppress first non-Exception shutdown cancellation; pre-existing non-Exception remains original.',
'SUP-002':'Explicit selector ownership preserves original cancellation through selector close failure, with best-effort annotation.',
'SUP-003':'One-shot owned process-group termination precedes direct-child reaping, independent of leader exit. Default SIGCHLD and exclusive unreaped child ownership are explicit preconditions.',
'SUP-004':'Dedicated cleanup lock serializes group action, stream cleanup and wait/reap. Contending cleanup uses its existing remaining deadline; timeout cannot report completed cleanup. Deterministic race now kill_entered → kill_completed → reap → reap.',
'worker':'Endpoint, complete C006 log/resource/span mapping, and cleanup cancellation call-site defects resolved by worker D; conditional begin/complete transport supervision reviewed against parent protocol.'},
'independentEvidence':{
'supervision':'36 finite adversarial checks plus 12 current author tests; same exact eight-source opening/closing vector.',
'worker':'18 real pinned SDK/inert-HTTP author tests; six original diagnostics-failure/cancellation counterexamples repaired; 428 independent projection checks on unchanged C→D mapping regions.',
'publicPorts':'23 SDK-free direct API controls; all 31 existing configuration/diagnostics class/function bodies remain AST-identical to pre-change HEAD.',
'ci':'Independent exact new job commands on Python3.11 with -S and ResourceWarning-as-error: 12 supervision tests with1 explicit API-dependency skip;18 worker tests with13 explicit SDK-dependency skips. These portable CI checks are distinct from actual pinned SDK source evidence.',
'resourceClosure':'Rehashed all698 previously installed wheel-owned files, all8 current source bytes and4 root test logs. No full interpreter/OS or installed application closure inferred.',
'artifacts':[pin('/private/tmp/astra-otel-supervision-review-20261010-c'+s) for s in ('.py','.json','.log')]+[pin('/private/tmp/astra-otel-public-ports-review-20261010-a'+s) for s in ('.py','.json','.log')]+[pin('/private/tmp/astra-otel-facade-context-controls-20261010-a'+s) for s in ('.py','.json','.log')]+[pin('/private/tmp/astra-otel-adapter-source-closure-20261010-b'+s) for s in ('.py','.json','.log')]},
'helixGuidelineDisposition':{
'modularity':'Parent facade owns explicit configuration, API contexts, clean subprocess environment, bounded protocol and process lifecycle. Private worker owns SDK construction, projections, queues and transport. New public configuration and sanitized validation ports match ADR map. Existing core/capture behavior preserved; no native/publication/ACK authority port added.',
'configuration':'Immutable explicit settings and finite budgets; no effective ambient OTEL/proxy settings through clean private worker environment. Secrets cross private initialization bytes rather than argv/environment/artifacts. Separate fixed initialization2s and failure cleanup<=2s are documented; shutdown retains its original selected deadline.',
'observability':'Exact event mapping and logical-unit loss accounting tested; no global SDK provider attachment, no claimed remote ingestion. Queue payload bounds remain separate from process/object memory. Actual receiver and installed workflow evidence required before integration support claims.',
'formal':'DIAG-F1/F2/F3/L1 are precise-only properties with reviewed ownership/state/transition correspondence. Counterexamples and affected repair controls retained. No analyzer/model proof, production scheduling bound or test-only assurance inflation.'},
'ciAndDocs':'One additive Python3.11 diagnostic-supervision job; existing CI bytes preserved. ADR frontmatter byte-identical, public ports/worker isolation/startup and process ownership assumptions explicit.',
'remainingRuntimeGates':['Fresh installed source/wheel correspondence and matching actual worker process imports.','Actual real HTTP receiver/protobuf and host capture composition, poisoned ambient environment, concurrent/retry/error/cancellation/outage/privacy and deadline controls.','No-active-valid-context behavior and failed-run pilot acceptance remain separately qualified future gates; source slice alone does not establish them.'],
'priorCounterexampleReceipts':[pin('/private/tmp/astra-otel-worker-source-review-20261010-a.json'),pin('/private/tmp/astra-otel-worker-cleanup-controls-20261010-c.json'),pin('/private/tmp/astra-otel-supervision-review-20261010-a.json'),pin('/private/tmp/astra-otel-supervision-review-20261010-b.json')],
'actions':'Read-only owner-source and evidence inspection; temporary reviewer artifacts and finite controlled tests only. No Git/source edits, network/receiver, secrets or native engines.'}
p=Path('/private/tmp/astra-otel-adapter-source-review-20261010-b.json');p.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(pin(p)))
