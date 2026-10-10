from pathlib import Path
import hashlib,json,subprocess
R=Path('/Users/erik/Projects/ashlar'); O=Path('/private/tmp/astra-otel-refusal-hold-review-20261010-a'); O.mkdir(exist_ok=False)
def desc(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
files=['src/ashlar_host/_otel_worker.py','tests/test_otel_worker.py','tests/test_otel_supervision.py']
source=(R/files[0]).read_text(); old=subprocess.check_output(['git','show','HEAD:'+files[0]],cwd=R).decode()
change="""            if worker is None:
                # Keep the init-refusal session owned and signalable until its
                # parent closes this private pipe or terminates the group.
                # Parent death supplies EOF; the parent owns the finite deadline.
                stdin.read(1)
"""
assert source.count(change)==1 and source.replace(change,'')==old
snapshots=[]
for rel in files:
 p=R/rel; q=O/p.name;q.write_bytes(p.read_bytes());snapshots.append(desc(q))
artifacts=[Path('/private/tmp/astra-otel-refusal-hold-independent-20261010-a'+s) for s in ('.py','.stdout.log','.stderr.log')]
report={'reviewer':'/root/astra_plan_review','verdict':'approve-exact-three-file-source-successor','scope':'Private init-refusal leader hold; source-only process ownership and cancellation, not live SDK worker or receiver qualification.','files':[desc(R/p) for p in files],'snapshots':snapshots,'implementationDelta':'Exactly five lines after successful fixed negative reply; only worker is None. Existing parent two-second init/disposal limits and honest unknown cleanup on denied group action are unchanged.','controls':{'command':'PYTHONPATH=src:tests /private/tmp/ashlar-otel-sdk-env-20261010-a/bin/python -B -S -W error::ResourceWarning /private/tmp/astra-otel-refusal-hold-independent-20261010-a.py','exitCode':0,'tests':7,'terminalChunk':'a76302','independent':'Constructor cancellation KI/SystemExit/GeneratorExit bypasses reply/hold; hold consumes exactly one byte after fixed negative frame; ready worker ordinary failure uses existing cleanup without init hold.','owner':'Fixed reply precedes hold/read cancellation; failed reply write never holds; real pipe EOF releases invalid-settings serve; real fake-constructor postadmission refusal remains live for owned parent kill/reap.','SDKLoaded':False},'artifacts':[desc(p) for p in artifacts],'findings':[],'assumptions':['Private pipe is not inherited by unrelated processes; parent death supplies EOF.','Exclusive POSIX process-group ownership/default SIGCHLD and finite parent supervision remain as previously reviewed.','In-process serve on a blocking pipe alone has no independent deadline; only the owning facade is the supported supervisor.'],'qualificationLimit':'No installed runtime, SDK provider, live receiver, network, native engine, or full C006 conformance executed.'}
out=O.with_suffix('.json');out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(desc(out)))
