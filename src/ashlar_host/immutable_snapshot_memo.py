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
    def __init__(self,owner):self.owner=owner;self.values={};self.value_capacities={};self.histories={};self.phase_witnesses={};self.bytes=0
    def _fits(self,capacity):return capacity<=1048576 and self.bytes+capacity+16384<=8*1048576
    def _retain_value(self,key,result):
        capacity=_size(result)+_size(key)
        if capacity>4*1048576:return
        protected=self.bytes-sum(self.value_capacities.values())
        if protected+capacity+16384>8*1048576:return
        retained=deepcopy(result)
        # Dict insertion order is the value-only LRU. Protected histories and
        # phase witnesses retain their original closing debt until interval exit.
        while self.values and (len(self.values)>=64 or self.bytes+capacity+16384>8*1048576):
            oldest=next(iter(self.values));del self.values[oldest]
            self.bytes-=self.value_capacities.pop(oldest)
        self.values[key]=retained;self.value_capacities[key]=capacity;self.bytes+=capacity

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
            retained=self.values.pop(key);self.values[key]=retained
            result=deepcopy(retained);owner._detail(target);return result
        result=owner._snapshot_uncached(target,version);owner._detail(target)
        # Oversize history disables observation reuse for that table. Its live
        # history still renews around each uncached read, as before.
        if table_key not in self.histories:
            closing=owner.original_history(target);owner._detail(target)
            from .driver import original_history_equal
            if not original_history_equal(closing,history):raise ValueError('Original immutable snapshot history changed')
        else:self._retain_value(key,result)
        return result
    def observe_history(self,owner,target):
        """Provisional committed-handle history; current identity stays live."""
        if owner is not self.owner:raise ValueError('Original immutable history owner required')
        table_key=(target.table,target.uuid,str(target.path))
        owner._detail(target)
        if table_key in self.histories:
            result=deepcopy(self.histories[table_key][1])
        else:
            result=owner.original_history(target)
            capacity=_size(result)+_size(table_key)+sys.getsizeof(target)+sys.getsizeof((target,result))
            if len(self.histories)<64 and self._fits(capacity):
                self.histories[table_key]=(target,deepcopy(result));self.bytes+=capacity
            else:
                # No retained reuse/debt for oversized histories: close this
                # individual observation independently before returning it.
                self.renew_history(target,result)
        owner._detail(target)
        return deepcopy(result)

    def phase_witness(self,owner,target,metadata):
        if owner is not self.owner:raise ValueError('Actual phase history owner required')
        if (type(metadata)is not tuple or not 1<=len(metadata)<=8
                or any(type(item)is not tuple or len(item)!=3 or any(type(v)is not str for v in item)for item in metadata)):
            raise ValueError('Exact immutable phase key metadata required')
        table_key=(target.table,target.uuid,str(target.path))
        self.observe_history(owner,target)
        if table_key not in self.histories:return None
        history=self.histories[table_key][1]
        versions=[row.get('version')for row in history]
        if not versions or any(type(v)is not int or v<0 for v in versions)or len(set(versions))!=len(versions):
            raise ValueError('Exact complete phase history versions required')
        version=max(versions);capacity=_size(metadata)+256
        if self.phase_witnesses or not self._fits(capacity):return None
        token=object();self.phase_witnesses[token]=(table_key,version,metadata);self.bytes+=capacity
        return token,version

    def admit_phase_witness(self,owner,target,witness):
        if owner is not self.owner or type(witness)is not tuple or len(witness)!=2:
            raise ValueError('Exact active phase history witness required')
        token,version=witness;retained=self.phase_witnesses.get(token)
        if retained is None or retained[:2]!=((target.table,target.uuid,str(target.path)),version):
            raise ValueError('Current retained phase history differs')
        owner._detail(target)
        return retained[2]

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
            memo.values.clear();memo.value_capacities.clear();memo.histories.clear();memo.phase_witnesses.clear();memo.bytes=0
