"""Full-vector PostgreSQL pin custody; external retention admission is mandatory.

Executor.transaction(context) must yield one authenticated transactional executor,
commit on normal exit, roll back on exceptions and keep locks throughout yield.
Never use an autocommit query adapter for this interface.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib,json,re
from types import MappingProxyType
from .publication import _name

class PinError(ValueError):
    pass

@dataclass(frozen=True)
class PinVector:
    authority: str
    scope_kind: str
    scope_id: str
    custody_digest: str
    targets: object

    def __post_init__(self):
        if any(not isinstance(x,str) or not x or len(x.encode())>1024 for x in [self.authority,self.scope_id]) or self.scope_kind not in ('manifest','cursor','recovery','release'):
            raise PinError('Explicit pin authority and scope required')
        if not isinstance(self.custody_digest,str) or not re.fullmatch('[0-9a-f]{64}',self.custody_digest):
            raise PinError('Original descriptor/request custody digest required')
        if not isinstance(self.targets,dict) or not 1<=len(self.targets)<=128:
            raise PinError('Complete bounded pin vector required')
        copy={}
        for table,target in self.targets.items():
            _name(table)
            if not isinstance(target,(tuple,list)) or len(target)!=2:
                raise PinError('Exact native UUID/version pair required')
            uuid,version=target
            if not isinstance(uuid,str) or not uuid or len(uuid.encode())>1024 or type(version) is not int or not 0<=version<2**63:
                raise PinError('Invalid pin UUID/version')
            copy[table]=(uuid,version)
        object.__setattr__(self,'targets',MappingProxyType(copy))

    def parameters(self,table):
        uuid,version=self.targets[table]
        # Stable within the original scope/table, independent of version/digest:
        # changed original descriptors conflict instead of allocating new pins.
        identity=[self.authority,self.scope_kind,self.scope_id,table]
        pin=hashlib.sha256(json.dumps(identity,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        return {'id':pin,'authority':self.authority,'kind':self.scope_kind,'scope':self.scope_id,
                'table':table,'uuid':uuid,'version':str(version),'digest':self.custody_digest}

class PostgresPins:
    def __init__(self,executor,policy):
        self.executor=executor;self.policy=policy
    @staticmethod
    def _call(session,function,vector,table):
        parameters=vector.parameters(table)
        session.query('SELECT ashlar_pins.'+function+'(:id,:authority,:kind,:scope,:table,:uuid,CAST(:version AS bigint),decode(:digest,\'hex\'))',parameters)
    @staticmethod
    def _inventory(session,vector):
        rows=session.query('SELECT table_name,table_uuid,version::text AS version,encode(custody_digest,\'hex\') AS digest,released FROM ashlar_pins.pin WHERE authority=:authority AND scope_kind=:kind AND scope_id=:scope LIMIT 129',
                           {'authority':vector.authority,'kind':vector.scope_kind,'scope':vector.scope_id}).rows
        if len(rows)!=len(vector.targets):raise PinError('Incomplete or ambiguous original scope vector')
        seen=set()
        for row in rows:
            table=row['table_name']
            if table in seen or table not in vector.targets or row['released'] is not False:
                raise PinError('Released, duplicate or foreign scope pin')
            seen.add(table);uuid,version=vector.targets[table]
            if (row['table_uuid'],row['version'],row['digest'])!=(uuid,str(version),vector.custody_digest):
                raise PinError('Original scope pin mismatch')
    def register(self,vector,*,context):
        # Admission must independently authenticate caller, bind complete exact
        # descriptor/source/effects, prove data availability and retention lane.
        if self.policy.admit_registration(vector,context) is not None:
            raise PinError('Pin registration admission incomplete')
        with self.executor.transaction(context) as session:
            for table in sorted(vector.targets):self._call(session,'register',vector,table)
            self._inventory(session,vector)
    @contextmanager
    def hold(self,vector,*,context):
        if self.policy.authorize_read(vector,context) is not None:
            raise PinError('Pin read authorization incomplete')
        with self.executor.transaction(context) as session:
            for table in sorted(vector.targets):self._call(session,'assert_active',vector,table)
            self._inventory(session,vector)
            yield vector
            # Recheck while shared lock still held; external authorization can
            # change independently from pin rows and must be renewed as well.
            if self.policy.authorize_read(vector,context) is not None:
                raise PinError('Pin read authorization expired')
            self._inventory(session,vector)
