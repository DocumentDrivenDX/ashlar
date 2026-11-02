# SPDX-License-Identifier: MIT OR Apache-2.0
"""Full-vector PostgreSQL pin custody; external retention admission is mandatory.

Executor.transaction(context) must yield one authenticated transactional executor,
commit on normal exit, roll back on exceptions and keep locks throughout yield.
Never use an autocommit query adapter for this interface.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib,json,re
import math,time
from types import MappingProxyType
from .publication import _name

INVENTORY_SQL = "SELECT table_name,table_uuid,version::text AS version,encode(custody_digest,'hex') AS digest,released FROM ashlar_pins.pin WHERE authority=:authority AND scope_kind=:kind AND scope_id=:scope LIMIT 129"
LIVE_PIN_TIMEOUT_SQL = "SELECT set_config(:setting, :timeout, true) AS applied"
LIVE_PIN_LIFETIME_SQL = (
    "SELECT extract(epoch FROM current_setting('idle_in_transaction_session_timeout')::interval)::double precision AS idle_timeout_seconds, "
    "extract(epoch FROM current_setting('transaction_timeout')::interval)::double precision AS transaction_timeout_seconds, "
    "extract(epoch FROM clock_timestamp() - transaction_timestamp())::double precision AS transaction_age_seconds"
)

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
    def _available(session,vector):
        for uuid in sorted({target[0] for target in vector.targets.values()}):
            session.query('SELECT ashlar_pins.assert_uuid_available(:uuid)',{'uuid':uuid})
    @staticmethod
    def _call(session,function,vector,table):
        parameters=vector.parameters(table)
        session.query('SELECT ashlar_pins.'+function+'(:id,:authority,:kind,:scope,:table,:uuid,CAST(:version AS bigint),decode(:digest,\'hex\'))',parameters)
    @staticmethod
    def _inventory(session,vector):
        rows=session.query(INVENTORY_SQL,
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
            self._available(session,vector)
            self._inventory(session,vector)
    @contextmanager
    def hold(self,vector,*,context):
        if self.policy.authorize_read(vector,context) is not None:
            raise PinError('Pin read authorization incomplete')
        with self.executor.transaction(context) as session:
            self._available(session,vector)
            for table in sorted(vector.targets):self._call(session,'assert_active',vector,table)
            self._inventory(session,vector)
            yield vector
            # Recheck while shared lock still held; external authorization can
            # change independently from pin rows and must be renewed as well.
            if self.policy.authorize_read(vector,context) is not None:
                raise PinError('Pin read authorization expired')
            self._available(session,vector)
            self._inventory(session,vector)

    @contextmanager
    def hold_page(self,vector,*,context):
        """Original held pin custody with typed pre-data unavailability.

        Only native explicit guard/permission refusals from this original pin
        admission are classified. Timeouts, connection failures and body errors
        retain their original classification and cause.
        """
        entered=False
        try:
            with self.hold(vector,context=context):
                entered=True
                yield vector
        except Exception as error:
            if not entered and getattr(error,'sqlstate',None)in('P0001','42501'):
                raise PinError('Original native page pin admission unavailable')from error
            raise

    def open_live(self,vector,source_selection,*,context,
                  bounded_lifetime_seconds=None):
        """Open an explicitly source-bound transaction-scoped pin handle.

        The policy must implement ``authorize_source_bound_read(vector,
        source_selection, context)`` and independently admit that exact source
        selection. This API checks pin identity/custody only; it does not prove
        retention duration or Delta-file readability.
        """
        if source_selection is None:
            raise PinError('Exact source selection required')
        if (bounded_lifetime_seconds is not None
                and (type(bounded_lifetime_seconds) is not int
                     or not 31 <= bounded_lifetime_seconds <= 900)):
            raise PinError('Bounded native pin transaction lifetime must be 31..900 seconds')
        authorize=getattr(self.policy,'authorize_source_bound_read',None)
        if not callable(authorize):
            raise PinError('Source-bound pin authorization unavailable')
        manager=self.executor.transaction(context)
        session=manager.__enter__()
        handle=_LivePinHandle(self,manager,session,vector,source_selection,
                              context,authorize,bounded_lifetime_seconds)
        try:
            if bounded_lifetime_seconds is not None:
                timeout=f'{bounded_lifetime_seconds * 1000}ms'
                for setting in ('idle_in_transaction_session_timeout',
                                'transaction_timeout'):
                    session.query(LIVE_PIN_TIMEOUT_SQL,
                                  {'setting':setting,'timeout':timeout})
            handle._check()
            if bounded_lifetime_seconds is not None:
                handle.native_pin_transaction_remaining_lifetime_seconds()
        except BaseException as error:
            handle._active=False
            _exit_preserving(manager,error,'Native pin open cleanup failed: ')
            raise
        return handle


def _exit_preserving(manager,error,note_prefix):
    try:manager.__exit__(type(error),error,error.__traceback__)
    except BaseException as cleanup:
        if cleanup is not error:
            add_note=getattr(error,'add_note',None)
            if callable(add_note):add_note(note_prefix+type(cleanup).__name__)


class _LivePinHandle:
    """Renewable checks on one original vector inside one held transaction."""
    def __init__(self,pins,manager,session,vector,source_selection,context,
                 authorize_source,bounded_lifetime_seconds):
        self._pins=pins;self._manager=manager;self._session=session
        self._vector=vector;self._source_selection=source_selection;self._context=context
        self._authorize_source=authorize_source;self._active=True
        self._bounded_lifetime_seconds=bounded_lifetime_seconds

    def _require_active(self):
        if not self._active:raise PinError('Live native pin handle is closed')

    def _check(self):
        self._require_active()
        if self._pins.policy.authorize_read(self._vector,self._context) is not None:
            raise PinError('Pin read authorization incomplete')
        if self._authorize_source(self._vector,self._source_selection,self._context) is not None:
            raise PinError('Source-bound pin authorization incomplete')
        self._pins._available(self._session,self._vector)
        for table in sorted(self._vector.targets):
            self._pins._call(self._session,'assert_active',self._vector,table)
        self._pins._inventory(self._session,self._vector)

    def native_pin_transaction_remaining_lifetime_seconds(self):
        """Read back effective PostgreSQL limits and transaction age in-session.

        Host round-trip time is subtracted from both server-derived bounds as a
        conservative allowance. An unconfigured or unsupported GUC refuses.
        """
        self._require_active()
        if self._bounded_lifetime_seconds is None:
            raise PinError('Native pin transaction lifetime admission unavailable')
        started=time.monotonic()
        rows=self._session.query(LIVE_PIN_LIFETIME_SQL,{}).rows
        ended=time.monotonic()
        elapsed=ended-started
        if (type(started) not in (int,float) or type(ended) not in (int,float)
                or not math.isfinite(started) or not math.isfinite(ended)
                or elapsed<0 or type(rows) is not list or len(rows)!=1
                or type(rows[0]) is not dict
                or set(rows[0])!={'idle_timeout_seconds','transaction_timeout_seconds',
                                  'transaction_age_seconds'}):
            raise PinError('Native pin transaction lifetime clock is invalid')
        idle=rows[0]['idle_timeout_seconds']
        transaction=rows[0]['transaction_timeout_seconds']
        age=rows[0]['transaction_age_seconds']
        if (any(type(value) not in (int,float) or not math.isfinite(value)
                for value in (idle,transaction,age))
                or idle != self._bounded_lifetime_seconds
                or transaction != self._bounded_lifetime_seconds
                or age < 0):
            raise PinError('Effective PostgreSQL transaction lifetime settings differ')
        idle_remaining=max(0.0,idle-elapsed)
        transaction_remaining=max(0.0,transaction-age-elapsed)
        return min(idle_remaining,transaction_remaining)

    def renew(self):
        """Recheck source authority and every exact pin on the held session."""
        try:self._check()
        except BaseException as error:
            self._active=False
            _exit_preserving(self._manager,error,'Native pin transaction exit failed: ')
            raise error

    def close(self):
        """Perform an independent final check, then end the transaction."""
        self._require_active()
        error=None
        try:self._check()
        except BaseException as caught:error=caught
        self._active=False
        if error is not None:
            _exit_preserving(self._manager,error,'Native pin transaction close failed: ')
            raise error
        self._manager.__exit__(None,None,None)

    def __enter__(self):
        self._require_active();return self

    def __exit__(self,exc_type,exc,tb):
        if not self._active:return False
        check_error=None
        try:self._check()
        except BaseException as error:check_error=error
        self._active=False
        if exc is not None:
            if check_error is not None:
                add_note=getattr(exc,'add_note',None)
                if callable(add_note):add_note('Native pin close check failed: '+type(check_error).__name__)
            _exit_preserving(self._manager,exc,'Native pin transaction close failed: ')
            return False
        if check_error is not None:
            _exit_preserving(self._manager,check_error,'Native pin transaction close failed: ')
            raise check_error
        self._manager.__exit__(None,None,None)
        return False
