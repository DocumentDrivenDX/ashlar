# SPDX-License-Identifier: MIT OR Apache-2.0
"""Live original commerce GraphFrames under actual ordinary publication custody.

Named carriers establish correspondence, not authority. The canonical reader and
isolated compatible GraphFrames worker retain distinct native vectors and lives.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from .publication_reader import OpenedCommerceReader
from .commerce_graphframes_scenarios import prepare_commerce_graphframes_profile
from .commerce_graph_release import hold_original_commerce_graph_release

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

@contextmanager
def hold_commerce_graphframes_session(session,pack_bytes):
 """Keep engine selection separate from the original neutral publication hold."""
 if type(session)is not CommerceGraphFramesSession or type(pack_bytes)is not bytes:raise ValueError('Exact selected commerce session required')
 session.__post_init__();opened=session.opened
 with hold_original_commerce_graph_release(opened)as held:
  profile=prepare_commerce_graphframes_profile(opened.admission,pack_bytes,held.release)
  yield held.release,profile,held.renew
