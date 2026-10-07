"""Closing gate refusals: prevent partial/aliased cohorts and same-version CID drift."""
import copy,json
from pathlib import Path
from types import SimpleNamespace
import publisher_closing_r548 as gate
B=Path(__file__).resolve().parent;s=json.loads((B/'out/native/closing_metadata_compare_r546/summary.json').read_text());expected=s['expected'];workers=[SimpleNamespace(out=Path('/private/tmp/closing-control-r550-'+str(i))) for i in range(4)];passed=[]
def refuse(label,w,e,phase='control'):
 try:gate.close(w,e,phase)
 except gate.CustodyMismatch:passed.append(label)
 else:raise AssertionError(label+' admitted')
refuse('shared client',workers[:3]+[workers[0]],expected)
refuse('missing role',workers,expected[:-1])
bad=copy.deepcopy(expected);bad[0]['head']['queryHistoryStatementId']=None;refuse('missing commit ID',workers,bad)
refuse('unsafe phase tag',workers,expected,'bad */')
original=gate.collect
def changed(w,e):
 result=copy.deepcopy(e);result[0]['head']['queryHistoryStatementId']='different-same-version-commit';return result
gate.collect=changed
try:refuse('changed commit despite same version',workers,expected)
finally:gate.collect=original
assert len(passed)==5
(B/'out/closing-controls-r550.json').write_text(json.dumps({'state':'Five closing-gate failures refuse without accepted partial cohort','controls':passed,'qualification':'Local invalid-shape and same-version commit-ID guard checks; no native writes, worker fencing or performance evidence.'},indent=2)+'\n');print(passed)
