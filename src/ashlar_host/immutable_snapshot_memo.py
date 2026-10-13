"""Bounded immutable observations within one admission; never cached authority."""
from contextlib import contextmanager
from copy import deepcopy
import sys

class ImmutableSnapshotMemo:
    def __init__(self,owner):self.owner=owner;self.values={};self.bytes=0
    def observe(self,owner,target,version):
        if owner is not self.owner or type(version)is not int or version<0:raise ValueError('Original immutable snapshot owner/version required')
        key=(target.table,target.uuid,str(target.path),version)
        def renew():
            owner._detail(target)
            history=owner.original_history(target)
            if not any(type(row)is dict and str(row.get('version'))==str(version)for row in history):raise ValueError('Original immutable snapshot version absent')
        renew()
        if key in self.values:
            result=deepcopy(self.values[key]);renew();return result
        result=owner._snapshot_uncached(target,version);renew()
        # Count recursive retained Python memory, not just serialized payload size.
        seen=set()
        def size(value):
            if id(value)in seen:return 0
            seen.add(id(value));total=sys.getsizeof(value)
            if type(value)is dict:total+=sum(size(k)+size(v)for k,v in value.items())
            elif type(value)in (tuple,list):total+=sum(size(v)for v in value)
            elif value is not None and type(value)not in (str,int,bool,bytes):raise ValueError('Exact snapshot cache carrier required')
            return total
        capacity=size(result)+size(key)
        if len(self.values)<64 and capacity<=1048576 and self.bytes+capacity+16384<=8*1048576:
            self.values[key]=deepcopy(result);self.bytes+=capacity
        return result

@contextmanager
def immutable_snapshot_memo(owner):
    if getattr(owner,'_immutable_snapshot_memo',None)is not None:raise ValueError('Distinct snapshot admission interval required')
    memo=ImmutableSnapshotMemo(owner);owner._immutable_snapshot_memo=memo
    try:yield memo
    finally:
        owner._immutable_snapshot_memo=None;memo.values.clear();memo.bytes=0
