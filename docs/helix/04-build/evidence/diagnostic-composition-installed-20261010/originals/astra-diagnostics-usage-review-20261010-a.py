from pathlib import Path
import hashlib,json,re,subprocess,tempfile
from ashlar_host.config import load_diagnostics_config, DiagnosticsLimits
R=Path('/Users/erik/Projects/ashlar');paths=[R/'examples/end-to-end/DIAGNOSTICS.md',R/'examples/end-to-end/README.md']
def d(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
text=paths[0].read_text();sample=re.search(r'```json\n(.*?)\n```',text,re.S).group(1);value=json.loads(sample)
assert set(value)=={'profile','capture_root','endpoint','headers','tls','ca_file','environment','limits'}
assert set(value['limits'])==set(DiagnosticsLimits.__dataclass_fields__) and len(value['limits'])==10
with tempfile.TemporaryDirectory(prefix='astra-diag-guide-') as root:
 p=Path(root).resolve()/'diagnostics.json';p.write_text(sample)
 config=load_diagnostics_config(p);assert len(config.origins)==17 and set(config.origins.values())=={'explicit'}
assert (paths[0].parent/'../../docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md').resolve().is_file()
assert '[diagnostics guide](DIAGNOSTICS.md)' in paths[1].read_text()
subprocess.run(['git','diff','--check','--',*[str(p) for p in paths]],cwd=R,check=True)
report={'reviewer':'/root/astra_plan_review','verdict':'approve-exact-two-file-usage-docs','files':[d(p) for p in paths],'checks':['Literal JSON has exactly eight loader-owned fields and all ten explicit limits. Actual SDK-free Python3.9 loader admits it with17 explicit origins.','Profile Python3.11/POSIX/optional extra selection matches public composition; native dependencies and compiler authority remain separate.','query-commerce-paths --diagnostics-config route and diagnostics retrieval options match actual CLI; filters compose conjunctively, severity17 allowed, reader limit50/10 valid.','Closed capture completeness is distinguished from business success and remote delivery; open/expired/invalid capture refuses.','Collector example uses loopback-test numeric localhost and three standard OTLP routes; remote TLS choices and secret file instructions match config contract.','Relative guide/contract links exist; diff check passes. No executed native, fallback-fault or full C006 support claim introduced.'],'sourceReferences':[d(R/'src/ashlar_host/config.py'),d(R/'src/ashlar/cli.py')],'scope':'Desired-state usage correctness and one synthetic config-file admission; no SDK/receiver/native execution.'}
p=Path('/private/tmp/astra-diagnostics-usage-review-20261010-a.json');p.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(d(p)))
