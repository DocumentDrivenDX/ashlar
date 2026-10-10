from pathlib import Path
import ast,hashlib,json,subprocess
R=Path('/Users/erik/Projects/ashlar');outdir=Path('/private/tmp/astra-paths-reader-final-review-20261010-d');outdir.mkdir(exist_ok=False)
def d(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
expected={'src/ashlar_host/paths_query.py':'71c090cdb05fbf30ce875a1189f16c045acad47fedf0efff33d617a6e3578d9c','src/ashlar_host/publication_reader.py':'d0693a36262871f5e4fe6335dadb7b67e5330ab1abe2b420d98724ade6b3ba5f','tests/test_host_paths_diagnostics.py':'32d45037986b860542397cfdff19fbdd73672e39e9c0d719c0bdbd89eda235b2'}
snap=[]
for rel,sha in expected.items():
 p=R/rel;assert d(p)['sha256']==sha;q=outdir/p.name;q.write_bytes(p.read_bytes());snap.append(d(q))
old=Path('/private/tmp/astra-paths-reader-predecessor-20261010-c/publication_reader.py').read_text();new=(R/'src/ashlar_host/publication_reader.py').read_text()
oldtail="""    except BaseException as error:
        primary = error
        raise
    finally:
        if transport is not None:
            if 'provider' in locals():
                provider._reader_closed = True
            _close_observed(transport.close, cleanup_state, primary)
"""
newtail="""    except BaseException as error:
        primary = error
    finally:
        actions = []
        if transport is not None:
            if 'provider' in locals():
                provider._reader_closed = True
            actions.append(lambda: _close_observed(transport.close, cleanup_state, primary))
        finish(primary, actions)
"""
assert old.count(oldtail)==1 and old.replace(oldtail,newtail)==new
subprocess.run(['git','diff','--check','--',*expected],cwd=R,check=True)
artifacts=[]
for prefix,suffixes in [('/private/tmp/astra-paths-reader-regression-predecessor-20261010-a',('.py','.stdout.log','.stderr.log')),('/private/tmp/astra-paths-reader-regression-20261010-g',('.stdout.log','.stderr.log')),('/private/tmp/astra-paths-reader-regression-20261010-e',('.stdout.log','.stderr.log')),('/private/tmp/astra-paths-reader-review-20261010-c',('.json',)),('/private/tmp/astra-paths-cleanup-independent-20261010-c',('.py','.json','.stdout.log','.stderr.log')),('/private/tmp/astra-paths-diagnostics-review-20261010-c',('.stdout.log','.stderr.log'))]:
 for suffix in suffixes:artifacts.append(d(Path(prefix+suffix)))
report={'reviewer':'/root/astra_plan_review','verdict':'approve-exact-three-file-paths-reader-source-successor','files':[d(R/p) for p in expected],'snapshots':snap,'scope':'Optional diagnostics phase hooks, actual reader/PG/Spark cleanup fact classification and corrected acquired reader primary preservation; source-only finite inert controls.','resolvedFindings':[{'id':'PATH-DIAG-001','resolution':'Closing custody checks re-enter closing phase after successful Spark cleanup.'},{'id':'PATH-DIAG-002','resolution':'Actual Spark.stop and reader transport/PG rollback/close facts classify cleanup failure independently of business exception markers. Closing ACK/native custody guards alone do not fabricate cleanup failure.'},{'id':'PATH-READER-PRIMARY-001','resolution':'Reader captures original primary and calls existing finish(primary,actions), even if no transport was acquired. Acquired transport close observes facts inside protected callback; primary is retained and native cleanup_failed marker follows existing owner policy.'}],'controls':[{'tests':38,'exitCode':0,'scope':'Predecessor fact-classification source/inert suite; unchanged Paths phase and fact code carried into final successor.'},{'independentWorkflowCases':4,'scope':'Spark/reader cleanup-only correct; closing-only false; state exact-type/singleclaim; provider guard/rollback/close method controls.'},{'command':'PYTHONPATH=src:tests /usr/bin/python3 -B -S -W error::ResourceWarning -m unittest discover -s tests -p test_host_paths_diagnostics.py','tests':18,'exitCode':0,'seconds':1.195,'terminalChunk':'1eae56','newRegression':'Eight subcases: observed/omitted state times ValueError/KeyboardInterrupt/SystemExit/GeneratorExit postyield primary plus close OSError. Assert guard actually reached, exact original identity, close once, cleanup marker and observed failed=true/cleanup_only=false.'},{'command':'PYTHONPATH=src:tests /usr/bin/python3 -B -S -W error::ResourceWarning /private/tmp/astra-paths-reader-regression-predecessor-20261010-a.py','exitCode':0,'expectedTestFailures':8,'scope':'Same durable test against exact retained f023b4 acquired-region AST. Every guard-reached assertion passes before original identity assertion fails; success assertion in driver confirms eight expected failures.'}],'correspondence':['Production successor exactly replaces the captured-primary/direct-final-close tail with primary-preserving existing finish; no other reader semantics edited.','No diagnostics None still omits state parameter and creates no SDK dependency; existing native ownership remains authoritative.','Only public read-only fact state crosses reader/Paths boundary. State stores booleans, no exception payload; one factory claim and exact type required.','Observation errors do not change business authority/report release; actual cleanup and closing checks remain required. Diagnostic first-cancellation policy and owning native primary policy remain distinct.'],'provenanceCorrection':'Earlier child seven-case fragment evidence lacked some inert prerequisite values, so it did not establish the selected postyield guard identity. Those historical bytes are retained but their cancellation attribution is superseded by the corrected parent durable regression and exact predecessor replay. Initial new-test fixture NameError log is likewise retained as reviewer setup failure, not product evidence.','artifacts':artifacts,'testAuthorship':'Reviewer authored only the requested durable acquired-region regression in tests/test_host_paths_diagnostics.py; root owns production fix. Parent root independently reran all97 focused controls after the test addition; that result is root-reported, not inferred from reviewer execution.','limits':['Acquired-region AST with inert ports is explicitly not whole-factory native execution.','No receiver, SDK worker, Spark, PostgreSQL, compiler, secrets or network used.','Precise semantic and implementation-control correspondence, not mechanical formal proof or full C006/native workflow qualification.']}
out=outdir.with_suffix('.json');out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(d(out)))
