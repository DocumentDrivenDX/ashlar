"""Conditional release-state abstraction plus independent Python helper replay.

No native authentication, complete carrier, policy, guard or sink theorem.
"""
import asyncio, hashlib, itertools, json
from pathlib import Path
import z3
from ashlar.weft_query import _completed_decode, _preserve_body_failure
from ashlar.publication import ResolutionError

root=Path(__file__).resolve().parent
admission,integrity,recheck,result_auth,top_deferred,close_failure,suppress=z3.Bools('admission integrity recheck result_auth top_deferred close_failure suppress')
body_failure=z3.Or(z3.Not(admission),z3.Not(integrity),z3.Not(recheck),z3.Not(result_auth),top_deferred)
# Abstract the Python wrapper's three exhaustive terminal paths: original body
# failure (propagated/restored), separate close failure, and successful close.
NONE,ORIGINAL,CLOSING=0,1,2
outcome=z3.If(body_failure,ORIGINAL,z3.If(close_failure,CLOSING,NONE))
release=outcome==NONE
original_has_close_cause=z3.And(body_failure,close_failure)
old_suppress_release=z3.And(z3.Not(close_failure),z3.Or(z3.Not(body_failure),suppress))
old_decode_body_failure=z3.Or(z3.Not(admission),z3.Not(integrity),z3.Not(recheck),z3.Not(result_auth))
old_decode_release=z3.And(z3.Not(old_decode_body_failure),z3.Not(close_failure))
old_close_outcome=z3.If(close_failure,CLOSING,z3.If(body_failure,ORIGINAL,NONE))
queries=[
 ('release-requires-all-local-gates',z3.And(release,z3.Or(z3.Not(admission),z3.Not(integrity),z3.Not(recheck),z3.Not(result_auth),top_deferred,close_failure)),'unsat'),
 ('original-failure-never-suppressed-or-replaced',z3.And(body_failure,outcome!=ORIGINAL),'unsat'),
 ('separate-closing-failure-retained-as-cause',z3.And(body_failure,close_failure,z3.Not(original_has_close_cause)),'unsat'),
 ('complete-local-path-remains-live',z3.And(admission,integrity,recheck,result_auth,z3.Not(top_deferred),z3.Not(close_failure),z3.Not(release)),'unsat'),
 ('old-suppression-release-counterexample',z3.And(admission,integrity,recheck,z3.Not(result_auth),z3.Not(top_deferred),suppress,old_suppress_release),'sat'),
 ('old-deferred-decoding-counterexample',z3.And(admission,integrity,recheck,result_auth,top_deferred,old_decode_release),'sat'),
 ('old-closing-replaces-original-counterexample',z3.And(body_failure,close_failure,old_close_outcome!=ORIGINAL),'sat'),
]
proof=[]
for name,formula,expected in queries:
 solver=z3.Solver();solver.add(formula);actual=str(solver.check());assert actual==expected,(name,actual)
 (root/(name+'.smt2')).write_text(solver.to_smt2())
 proof.append({'id':name,'expected':expected,'observed':actual,'model':str(solver.model()) if actual=='sat' else None})

class Abort(BaseException):pass
controls=[]
for body,decode,closing in itertools.product(['complete','exception','abort'],['complete','generator','coroutine','async-generator','awaitable'],['complete','suppress','fail']):
 events=[];original=PermissionError('original') if body=='exception' else Abort('original') if body=='abort' else None
 close_error=ResolutionError('closing');captured=None;value=None;returned=False;raised=None
 class Manager:
  def __enter__(self):events.append('enter')
  def __exit__(self,*error):
   events.append('exit')
   if closing=='fail':raise close_error
   return closing=='suppress'
 def generator():events.append('deferred body');yield 'future'
 async def coroutine():events.append('deferred body');return 'future'
 async def async_generator():events.append('deferred body');yield 'future'
 class Awaitable:
  def __await__(self):events.append('deferred body');yield 'future'
 try:
  with _preserve_body_failure(Manager()):
   if original is not None:raise original
   value={'complete':'exact','generator':generator,'coroutine':coroutine,'async-generator':async_generator,'awaitable':Awaitable}[decode]
   if callable(value):value=value()
   try:_completed_decode(value)
   except BaseException as error:captured=error;raise
  returned=True
 except BaseException as error:raised=error
 expected_return=body=='complete' and decode=='complete' and closing!='fail'
 assert returned==expected_return
 expected_original=original if original is not None else captured
 if expected_original is not None:
  assert raised is expected_original
  if closing=='fail':assert raised.__cause__ is close_error
 elif closing=='fail':assert raised is close_error
 assert events==['enter','exit'],events
 if decode=='generator' and value is not None:assert value.gi_frame is None
 if decode=='coroutine' and value is not None:assert value.cr_frame is None
 if decode=='async-generator' and value is not None:asyncio.run(value.aclose())
 controls.append({'body':body,'decode':decode,'closing':closing,'returned':returned,'originalPreserved':expected_original is None or raised is expected_original,'exitCount':events.count('exit')})
(root/'results.json').write_text(json.dumps({'z3Version':z3.get_version_string(),'proofQueries':proof,'helperControls':controls,'scope':'Seven conditional local release-state queries (4 UNSAT/3 SAT) and 45 exhaustive selected helper controls. Assumes local gates classify errors correctly and original context lifecycle is the Python with protocol. Does not prove authentic gates, original native authority/caller/source, complete recursive carriers, interval acquisition/cleanup correctness, SQL semantics, final sink custody or backend acceptance.'},indent=2)+'\n')
print(json.dumps({'queries':len(proof),'unsat':4,'sat':3,'helperControls':len(controls),'result':'pass'}))
