"""Original commerce graph bytes inside actual ordinary publication custody.

This neutral hold opens no engine and grants no export or deployment authority.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import json
from ashlar.graph_release import GraphRelease, read_graph_release
from ashlar.manifest import manifest_pin_vector
from .publication_reader import OpenedCommerceReader
from collections import Counter
from .supply_chain_request import source_snapshot
from .delta_custody import encoded
from .lifecycle import finish

class _CommerceGraphPolicy:
 def __init__(self,opened,vector):
  if type(opened)is not OpenedCommerceReader:raise ValueError('Exact original commerce owner required')
  self.opened=opened;self.vector=vector
  self.manifest=encoded(opened.manifest);self.expected=source_snapshot(opened.independent_expected)
  self.roles={opened.driver.tables[role]:role for role in ('object_current','edge_current')}
  self.ports=(opened.provider,opened.driver,opened.context,opened.admission,opened.driver.transport,opened.driver.policy)
  self.rows={role:{encoded(row)for row in self.expected[role]}for role in ('object_current','edge_current')}
 def local(self,context):
  opened=self.opened
  if (context is not self.ports[2] or not opened.provider.active or opened.provider._reader_closed
      or any(current is not original for current,original in zip((opened.provider,opened.driver,opened.context,opened.admission,opened.driver.transport,opened.driver.policy),self.ports))
      or opened.driver.source_admission is not self.ports[3]):raise PermissionError('Original active commerce graph interval required')
  current=manifest_pin_vector(self.opened.manifest,{table:self.opened.driver.transport.targets[table].uuid for table in json.loads(self.opened.manifest['table_versions_json'])},authority='private-local-process')
  if current!=self.vector:raise PermissionError('Original complete current held vector differs')
  self.opened.driver.authorize(context,self.opened.manifest['publication_id'],tuple(self.vector.targets))
 def require(self,context):
  self.opened.renew_original_graph_read(self.vector,context=context)
  self.local(context)
 def bind_descriptor(self,descriptor,vector,context):
  self.local(context)
  if vector!=self.vector or encoded(dict(descriptor.raw))!=self.manifest:raise PermissionError('Original whole descriptor/vector differs')
 def authorize_graph_row(self,descriptor,table,row,context):
  self.bind_descriptor(descriptor,self.vector,context)
  role=self.roles.get(table)
  if role is None or encoded(dict(row))not in self.rows[role]:raise PermissionError('Independent original complete row differs')
 def authorize_graph_result(self,descriptor,nodes,edges,context):
  self.bind_descriptor(descriptor,self.vector,context)
  for role,rows in (('object_current',nodes),('edge_current',edges)):
   canonical=[{k:v for k,v in row.items()if k not in ('graph_id','src','dst')}for row in rows]
   if Counter(encoded(row)for row in canonical)!=Counter(encoded(row)for row in self.expected[role]):raise PermissionError('Independent original full carrier occurrence bag differs')

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
 policy.local(opened.context)
 return read_graph_release(opened.driver.transport,opened.driver,_HeldCommercePins(policy),policy.vector,policy,
  publication_id=opened.manifest['publication_id'],node_table=opened.driver.tables['object_current'],edge_table=opened.driver.tables['edge_current'],context=opened.context,
  supported_profiles=['ashlar-delta/0.3'],supported_revisions={k:[v]for k,v in json.loads(opened.manifest['schema_revisions_json']).items()},
  max_nodes=len(opened.independent_expected['object_current']),max_edges=len(opened.independent_expected['edge_current']))

class _IntervalLife:
 def __init__(self):self.active=True

@dataclass(frozen=True)
class HeldOriginalCommerceGraphRelease:
 """Immutable carrier and renewal of this actual interval, never a new grant."""
 release:GraphRelease
 _policy:_CommerceGraphPolicy
 _life:_IntervalLife
 def __post_init__(self):
  if type(self.release)is not GraphRelease or type(self._policy)is not _CommerceGraphPolicy or type(self._life)is not _IntervalLife:
   raise ValueError('Exact original held commerce release required')
 def renew(self):
  if not self._life.active:raise PermissionError('Original commerce graph interval released')
  self._policy.require(self._policy.opened.context)

@contextmanager
def hold_original_commerce_graph_release(opened):
 """Retain real source/ACK/native custody through independent full closing."""
 if type(opened)is not OpenedCommerceReader:raise ValueError('Exact original commerce reader required')
 if opened.provider._reader_closed or opened.provider.driver is not opened.driver or opened.provider.context is not opened.context or opened.driver.source_admission is not opened.admission:
  raise PermissionError('Original reader owner correspondence required')
 if opened._correspondence()!=opened._original:raise PermissionError('Original reader correspondence changed')
 life=_IntervalLife()
 try:
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
    yield HeldOriginalCommerceGraphRelease(release,policy,life)
   except BaseException as error:primary=error
   finish(primary,[lambda:opened.require_current(opened.context),close_native])
  opened.provider.closed_interval_custody(opened.context)
 finally:life.active=False


def read_commerce_export(reader_factory):
 """Return original release/custody only after the enclosing reader closes.

 The factory must open the configured public commerce reader and own its outer
 resources. This function uses the genuine held source/ACK/native release ports;
 it neither constructs observations nor writes provisional export files.
 """
 from .lifecycle import owned_context
 from .private_graph_custody import build_private_custody
 completed=False;release=None;capture=None;source=None;original_native=None
 with owned_context(reader_factory())as opened:
  with owned_context(hold_original_commerce_graph_release(opened))as held:
   release=held.release
  capture=source_snapshot(opened.provider.closed_interval_custody(opened.context))
  required={'format','scope','source_schema','source_signature_sha256','original_request_hex','original_manifest_hex','opening_ack','closing_ack','qualification'}
  if type(capture)is not dict or set(capture)!=required or capture['format']!='ashlar-private-local-protected-ack-interval/0.1':
   raise PermissionError('Exact closed ordinary ACK capture required')
  for phase in ('opening_ack','closing_ack'):
   session=capture[phase]['session']
   if session['source_schema']!=capture['source_schema']or session['source_signature_sha256']!=capture['source_signature_sha256']:
    raise PermissionError('Original ordinary source session differs')
  source=source_snapshot(opened.admission.metadata());original_native=dict(opened.original_native_files);completed=True
 if not completed or release is None:raise PermissionError('Complete original export did not close')
 ack={'scope':capture['scope'],'requestHex':capture['original_request_hex'],'manifestHex':capture['original_manifest_hex'],
  'opening':capture['opening_ack'],'closing':capture['closing_ack']}
 custody=build_private_custody(release,source,ack)
 return release,custody,original_native
