"""Live original commerce GraphFrames under actual ordinary publication custody.

Named carriers establish correspondence, not authority. The canonical reader and
isolated compatible GraphFrames worker retain distinct native vectors and lives.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import json
from ashlar.graph_release import read_graph_release
from ashlar.manifest import manifest_pin_vector
from .publication_reader import OpenedCommerceReader
from .commerce_graphframes_scenarios import prepare_commerce_graphframes_profile
from .graphframes_pack_scenarios import bag
from .supply_chain_request import source_snapshot
from .delta_custody import encoded
from .lifecycle import finish

@dataclass(frozen=True)
class CommerceGraphFramesSession:
 opened:OpenedCommerceReader
 runtime_policy:object
 runtime_context:object
 def __post_init__(self):
  if type(self.opened)is not OpenedCommerceReader:raise ValueError('Exact actual original commerce reader required')
  if not all(callable(getattr(self.runtime_policy,n,None))for n in ('admit_graphframes_runtime','renew_graphframes_runtime')):raise ValueError('Actual complete installed runtime custody required')
  opened=self.opened
  if opened.provider._reader_closed or opened.provider.driver is not opened.driver or opened.provider.context is not opened.context or opened.driver.source_admission is not opened.admission:raise PermissionError('Original reader owner correspondence required')
  if opened._correspondence()!=opened._original:raise PermissionError('Original reader correspondence changed')

class _CommerceGraphPolicy:
 def __init__(self,opened,vector):
  if type(opened)is not OpenedCommerceReader:raise ValueError('Exact original commerce owner required')
  self.opened=opened;self.vector=vector
  self.manifest=encoded(opened.manifest);self.expected=source_snapshot(opened.independent_expected)
  self.roles={opened.driver.tables[role]:role for role in ('object_current','edge_current')}
 def require(self,context):
  self.opened.renew_original_graph_read(self.vector,context=context)
  current=manifest_pin_vector(self.opened.manifest,{table:self.opened.driver.transport.targets[table].uuid for table in json.loads(self.opened.manifest['table_versions_json'])},authority='private-local-process')
  if current!=self.vector:raise PermissionError('Original complete current held vector differs')
  self.opened.driver.authorize(context,self.opened.manifest['publication_id'],tuple(self.vector.targets))
 def bind_descriptor(self,descriptor,vector,context):
  self.require(context)
  if vector!=self.vector or encoded(dict(descriptor.raw))!=self.manifest:raise PermissionError('Original whole descriptor/vector differs')
 def authorize_graph_row(self,descriptor,table,row,context):
  self.bind_descriptor(descriptor,self.vector,context)
  role=self.roles.get(table)
  if role is None or encoded(dict(row))not in {encoded(v)for v in self.expected[role]}:raise PermissionError('Independent original complete row differs')
 def authorize_graph_result(self,descriptor,nodes,edges,context):
  self.bind_descriptor(descriptor,self.vector,context)
  for role,rows in (('object_current',nodes),('edge_current',edges)):
   canonical=[{k:v for k,v in row.items()if k not in ('graph_id','src','dst')}for row in rows]
   if bag(canonical)!=bag(self.expected[role]):raise PermissionError('Independent original full carrier occurrence bag differs')

class _HeldCommercePins:
 """Correspondence to one actual acquired NativeDriver hold; no new grant."""
 def __init__(self,policy):
  if type(policy)is not _CommerceGraphPolicy:raise ValueError('Closed actual graph policy required')
  self.policy=policy
 @contextmanager
 def hold(self,vector,*,context):
  self.policy.require(context)
  if vector!=self.policy.vector:raise PermissionError('Exact already held original vector required')
  primary=None
  try:yield
  except BaseException as error:primary=error
  finish(primary,[lambda:self.policy.require(context)])

def _read_release(opened,policy):
 policy.require(opened.context)
 return read_graph_release(opened.driver.transport,opened.driver,_HeldCommercePins(policy),policy.vector,policy,
  publication_id=opened.manifest['publication_id'],node_table=opened.driver.tables['object_current'],edge_table=opened.driver.tables['edge_current'],context=opened.context,
  supported_profiles=['ashlar-delta/0.3'],supported_revisions={k:[v]for k,v in json.loads(opened.manifest['schema_revisions_json']).items()},
  max_nodes=len(opened.independent_expected['object_current']),max_edges=len(opened.independent_expected['edge_current']))

@contextmanager
def hold_commerce_graphframes_session(session,pack_bytes):
 """Acquire actual commerce read authority; renewal is owned by this session."""
 if type(session)is not CommerceGraphFramesSession or type(pack_bytes)is not bytes:raise ValueError('Exact selected commerce session required')
 session.__post_init__();opened=session.opened
 with opened.provider.interval(opened.context):
  primary=None;release=None;policy=None
  def current_policy():
   return _CommerceGraphPolicy(opened,manifest_pin_vector(opened.manifest,{table:opened.driver.transport.targets[table].uuid for table in json.loads(opened.manifest['table_versions_json'])},authority='private-local-process'))
  def close_native():
   closing=_read_release(opened,policy if policy is not None else current_policy())
   if release is not None and closing!=release:raise PermissionError('Closing original whole graph release changed')
  try:
   opened.require_current(opened.context)
   policy=current_policy();release=_read_release(opened,policy)
   profile=prepare_commerce_graphframes_profile(opened.admission,pack_bytes,release)
   def renew():
    # Exact whole native bytes corroborate this carrier only inside this actual
    # interval; full raw independent bags and native snapshots repeat at closing.
    policy.require(opened.context)
   yield release,profile,renew
  except BaseException as error:primary=error
  finish(primary,[lambda:opened.require_current(opened.context),close_native])
 opened.provider.closed_interval_custody(opened.context)
