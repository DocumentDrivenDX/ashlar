# SPDX-License-Identifier: MIT OR Apache-2.0
"""Zero-only future phase observation in one actual read-only ledger interval.

A raw row disables reuse and falls back to normal source-scoped validation.
Positive original reservation, effect absence and source/ACK remain separate.
"""
import json
from ashlar.phase_read import PhaseReadRequest,PhaseAbsence


class PhaseAbsenceOwner:
    def __init__(self,driver,original,inventory,correspondence,context):
        from .driver import NativeDriver
        from .delta_custody import LocalDeltaTransport
        from .evolution_run import EvolutionRunDefinition,EvolutionRunLedgerView
        from .evolution_composition import OriginalRunCorrespondence
        if (type(driver)is not NativeDriver or type(original)is not EvolutionRunDefinition
                or type(inventory)is not EvolutionRunLedgerView or type(correspondence)is not OriginalRunCorrespondence
                or correspondence.original is not original or context is not driver.context):
            raise PermissionError('Actual original ledger phase owner required')
        self.driver=driver;self.original=original;self.raw=original.raw
        self.inventory=inventory;self.slots=inventory.slots;self.context=context
        self.active=True;self.disabled=type(driver.transport)is not LocalDeltaTransport
        self.witness=None;self.empty=None;self.requests={}
        self.table=driver.tables['attempts'];self.target=None
        if self.disabled:return
        if type(self.slots)is not tuple or len(self.slots)!=8:
            raise PermissionError('Complete original phase inventory required')
        self.target=driver.transport.targets[self.table]
        self.identity=(self.target.table,self.target.uuid,str(self.target.path))
        for ordinal,state,raw,digest,descriptor in self.slots:
            if state!='unprepared':continue
            if raw is not None or digest is not None or descriptor is not None:
                raise PermissionError('Strictly unprepared original phase slot required')
            request=correspondence.at(ordinal)['request']
            key=(request['stream'],request['batch_id'])
            text=json.dumps(request,sort_keys=True,separators=(',',':'))
            if key in self.requests:
                # A shared stream/batch can contain independent feed/epoch groups.
                # Ambiguous candidate keys disable this zero-only optimization;
                # ordinary core scoped reads retain their complete validation.
                self.disabled=True;self.requests.clear();return
            self.requests[key]=text
        self.metadata=tuple((stream,batch,text)for (stream,batch),text in self.requests.items())
        keys=tuple(self.requests)
        if (not keys or len(keys)>8 or len({key[0]for key in keys})!=1
                or any(not 0<len(value.encode())<=1024 for key in keys for value in key)
                or sum(len(text.encode())for text in self.requests.values())>1048576):
            self.disabled=True;self.requests.clear()

    def observe(self,read,*,context):
        if type(read)is not PhaseReadRequest:raise PermissionError('Actual core phase request required')
        read.renew(context)
        if not self.active:return None
        if context is not self.context or read.context is not self.context or self.driver.context is not self.context:
            raise PermissionError('Original held phase context differs')
        if self.original.raw!=self.raw or self.inventory.slots!=self.slots:
            raise PermissionError('Original phase inventory changed')
        if self.disabled:return None
        if tuple((stream,batch,text)for (stream,batch),text in self.requests.items())!=self.metadata:
            raise PermissionError('Original phase request inventory changed')
        target=self.driver.transport.targets[self.table]
        if (target.table,target.uuid,str(target.path))!=self.identity:
            raise PermissionError('Original phase table identity changed')
        if (read.table,read.uuid)!=(target.table,target.uuid):
            raise PermissionError('Original core phase target differs')
        key=(read.stream,read.batch_id)
        if key not in self.requests:return None
        if read.original_request_json!=self.requests[key]:
            raise PermissionError('Original future phase request changed')
        if self.empty is None:
            metadata=self.metadata
            self.witness=self.driver.transport.retain_phase_absence_witness(self.table,metadata)
            if self.witness is None:self.disabled=True;return None
            rows=self.driver.transport.observe_phase_absence_keys(self.table,self.witness,key[0],tuple(k[1]for k in self.requests))
            if type(rows)is not list:raise PermissionError('Exact native phase candidate rows required')
            if rows:
                self.disabled=True;self.empty=False;return None
            self.empty=True
        self.driver.transport.renew_phase_absence_witness(self.table,self.witness,metadata=self.metadata)
        read.renew(context)
        return PhaseAbsence(read)

    def close(self):
        self.active=False;self.requests.clear();self.witness=None;self.empty=None
