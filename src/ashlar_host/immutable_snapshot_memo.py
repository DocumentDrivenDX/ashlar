"""Bounded observations of immutable Delta versions in one ledger admission.

Results stay provisional until full history is freshly revalidated on exit.
This local interval assumes Delta versions are immutable; it caches no source,
ACK, UUID authority or history observations across admissions.
"""
from contextlib import contextmanager
from copy import deepcopy
import sys
from .lifecycle import finish

def _size(value):
    seen=set()
    def size(item):
        if id(item)in seen:return 0
        seen.add(id(item));total=sys.getsizeof(item)
        if type(item)is dict:total+=sum(size(k)+size(v)for k,v in item.items())
        elif type(item)in (tuple,list):total+=sum(size(v)for v in item)
        else:
            # Native history also contains datetime and float carriers. They
            # are immutable leaves, compared by the existing native comparator.
            from datetime import datetime
            if item is not None and type(item)not in (str,int,bool,bytes,float,datetime):raise ValueError('Exact snapshot cache carrier required')
        return total
    return size(value)

class ImmutableSnapshotMemo:
    def __init__(self,owner):self.owner=owner;self.values={};self.histories={};self.bytes=0
    def _fits(self,capacity):return capacity<=1048576 and self.bytes+capacity+16384<=8*1048576
    def observe(self,owner,target,version):
        if owner is not self.owner or type(version)is not int or version<0:raise ValueError('Original immutable snapshot owner/version required')
        table_key=(target.table,target.uuid,str(target.path));key=(*table_key,version)
        owner._detail(target)
        if table_key in self.histories:history=self.histories[table_key][1]
        else:
            history=owner.original_history(target)
            capacity=_size(history)+_size(table_key)+sys.getsizeof(target)+sys.getsizeof((target,history))
            if len(self.histories)<64 and self._fits(capacity):
                self.histories[table_key]=(target,deepcopy(history));self.bytes+=capacity
        if not any(type(row)is dict and str(row.get('version'))==str(version)for row in history):raise ValueError('Original immutable snapshot version absent')
        if key in self.values:
            result=deepcopy(self.values[key]);owner._detail(target);return result
        result=owner._snapshot_uncached(target,version);owner._detail(target)
        capacity=_size(result)+_size(key)
        # Oversize history disables observation reuse for that table. Its live
        # history still renews around each uncached read, as before.
        if table_key not in self.histories:
            closing=owner.original_history(target);owner._detail(target)
            from .driver import original_history_equal
            if not original_history_equal(closing,history):raise ValueError('Original immutable snapshot history changed')
        elif len(self.values)<64 and self._fits(capacity):
            self.values[key]=deepcopy(result);self.bytes+=capacity
        return result
    def renew_history(self,target,history):
        from .driver import original_history_equal
        self.owner._detail(target)
        closing=self.owner.original_history(target)
        self.owner._detail(target)
        if not original_history_equal(closing,history):raise ValueError('Original immutable snapshot history changed')

@contextmanager
def immutable_snapshot_memo(owner):
    if getattr(owner,'_immutable_snapshot_memo',None)is not None:raise ValueError('Distinct snapshot admission interval required')
    memo=ImmutableSnapshotMemo(owner);owner._immutable_snapshot_memo=memo
    primary=None
    try:
        yield memo
    except BaseException as error:primary=error
    finally:
        owner._immutable_snapshot_memo=None
        try:
            finish(primary,[lambda target=target,history=history:memo.renew_history(target,history)for target,history in memo.histories.values()])
        finally:
            memo.values.clear();memo.histories.clear();memo.bytes=0
