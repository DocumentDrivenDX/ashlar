from pathlib import Path
import hashlib,json,subprocess
R=Path('/Users/erik/Projects/ashlar'); files=['docs/helix/02-design/contracts/CONTRACT-006-diagnostics.md','docs/helix/02-design/adr/ADR-001-delta-canonical-and-serving-layout.md','docs/helix/03-test/consumer-conformance-plan.md']
def desc(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
for f in files:
 old=subprocess.check_output(['git','show','HEAD:'+f],cwd=R);new=(R/f).read_bytes()
 assert old.split(b'---',2)[:2]==new.split(b'---',2)[:2]
subprocess.run(['git','diff','--check','--',*files],cwd=R,check=True)
report={'reviewer':'/root/astra_plan_review','verdict':'approve-exact-three-document-desired-state-delta','files':[desc(R/f) for f in files],'scope':'Optional ReaderCleanupState ownership and diagnostic fact classification only; source review and native workflow evidence separate.','findings':[],'checks':['Exact-type optional state, read-only payload-free failure booleans, fresh reader factory claim and provider sharing are owned by publication_reader.','Actual transport.close and PostgreSQL rollback/close failures are distinguished from custody/ACK guard failures. First actual cleanup failure records whether a primary was already active.','Observation confers no authority or alternate cleanup policy; omitted state preserves existing lifecycle.','ADR module table names the public fact port; test plan requires exact admission, immutable facts, single claim and both actual method-fault distinctions.','No duplicated assurance claim, implemented receiver/native coverage, or mechanical formal proof is introduced; frontmatter unchanged and diff-check passed.'],'qualification':'Desired-state semantic approval; does not approve pending source counterexample disposition or infer full C006 support.'}
out=Path('/private/tmp/astra-reader-cleanup-facts-governance-review-20261010-a.json');out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(desc(out)))
