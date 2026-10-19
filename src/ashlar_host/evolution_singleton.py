# SPDX-License-Identifier: MIT OR Apache-2.0
"""Two-source original publication singleton reads under actual local custody.

Protected receipt reconciliation uses existing ordinary SELECT-lock transactions;
it never submits an ACK. No production fencing or remote authority is asserted.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import json
from ashlar.singleton import read_singleton
from .driver import NativeDriver, HeldEvolutionPublication
from .delta_custody import encoded
from .evolution_composition import NativeEvolutionRunPolicy
from .evolution_operator import ExistingEvolutionLease
from .evolution_run import EvolutionRunDefinition
from .evolution_verification_types import VerifyEvolutionInvocation, VerifyCommerceEvolutionConfig
from .source_sessions import RegisteredOutboxSources
from .ack_sessions import RegisteredOutboxAcks
from .lifecycle import finish, owned_context
from .exact_report import report_body


@dataclass(frozen=True)
class EvolutionSingletonSelection:
    verification: VerifyEvolutionInvocation
    kind: str
    type_id: int
    entity_id: int

    def __post_init__(self):
        if (type(self.verification)is not VerifyEvolutionInvocation or type(self.kind)is not str
                or self.kind not in ('object','edge')
                or any(type(v)is not int or not -(2**63)<=v<2**63 for v in (self.type_id,self.entity_id))):
            raise PermissionError('Exact original publication and typed singleton identity required')
        self.verification.__post_init__()


@dataclass(frozen=True)
class _BorrowedPins:
    held: HeldEvolutionPublication
    def __post_init__(self):
        if type(self.held)is not HeldEvolutionPublication:raise PermissionError('Actual held publication required')
    @contextmanager
    def hold(self, vector, *, context):
        self.held.require(context,vector);primary=None
        try:yield
        except BaseException as error:primary=error
        finish(primary,[lambda:self.held.require(context,vector)])


@dataclass(frozen=True)
class _SingletonPolicy:
    held: HeldEvolutionPublication
    selection: EvolutionSingletonSelection
    feed: str
    def __post_init__(self):
        if type(self.held)is not HeldEvolutionPublication or type(self.selection)is not EvolutionSingletonSelection:
            raise PermissionError('Nominal original read owners required')
    def bind_descriptor(self, descriptor, vector, context):
        self.held.require(context,vector)
        if encoded(dict(descriptor.raw))!=encoded(self.held.descriptor):
            raise PermissionError('Complete original descriptor differs')
    def authorize_row(self, descriptor, table, row, context):
        self.bind_descriptor(descriptor,self.held.vector,context)
        role='object_current'if self.selection.kind=='object'else'edge_current'
        if table!=self.held.driver.tables[role]:raise PermissionError('Original singleton role differs')
        typed='type_id'if self.selection.kind=='object'else'rel_type_id'
        rows=[r for r in self.held.facts['expected'][role]if
            (r['source_system'],r[typed],r['id'])==(self.feed,str(self.selection.type_id),str(self.selection.entity_id))]
        if len(rows)>1 or (row is None and rows)or(row is not None and (len(rows)!=1 or dict(row)!=rows[0])):
            raise PermissionError('Independent complete original singleton row/absence differs')


def read_evolution_singletons(selection, provider):
    """Return bounded immutable JSON only after all actual provider owners close.

    The explicitly trusted provider is the existing public verification provider;
    its actual nominal driver/lease/source/ACK owners must match the selection.
    Both registered feeds are read for the SAME typed identity, including absence.
    """
    if type(selection)is not EvolutionSingletonSelection:
        raise PermissionError('Nominal original singleton selection required')
    selection.__post_init__();invocation=selection.verification;answer=None
    with owned_context(provider.open_verification(invocation))as config:
        if (type(config)is not VerifyCommerceEvolutionConfig or type(config.driver)is not NativeDriver
                or type(config.policy)is not NativeEvolutionRunPolicy or config.policy.driver is not config.driver
                or type(config.policy.authority)is not ExistingEvolutionLease
                or type(config.original)is not EvolutionRunDefinition or config.original.sha256!=invocation.expected_sha256
                or config.policy.authority.context is not config.context
                or config.policy.authority.invocation is not invocation
                or any(getattr(config,k)!=getattr(invocation,k)for k in
                    ('ledger_path','producer','expected_sha256','ordinal','bounds','supervision','native_schema_profile'))):
            raise PermissionError('Exact original live singleton provider owners required')
        config.__post_init__();driver=config.driver;policy=config.policy;context=config.context
        if (type(driver.source_sessions)is not RegisteredOutboxSources
                or type(driver.ack_sessions)is not RegisteredOutboxAcks
                or len(driver.source_sessions.registrations)!=2):
            raise PermissionError('Actual original two-source registrations required')
        feeds=tuple(i.scope.feed for i in driver.source_sessions.registrations)
        if len(set(feeds))!=2 or {i.source.scope for i in driver.ack_sessions.registrations}!={i.scope for i in driver.source_sessions.registrations}:
            raise PermissionError('Both actual source/ACK scopes must correspond')
        with owned_context(driver.writer('original-evolution-singleton',context)):
            opening=None;primary=None
            def renew_local():
                fresh=policy.authority.inspect_ledger(config.original,context)
                if opening is not None and fresh!=opening:raise PermissionError('Original complete ledger drifted')
            def renew_native():
                fresh=policy.authority.inspect_ledger(config.original,context)
                policy.admit_ledger(config.original,fresh,context)
            try:
                opening=policy.authority.inspect_ledger(config.original,context)
                with owned_context(driver.hold_evolution_publication(config.original,opening,invocation.ordinal,
                        context=context,policy=policy,bounds=config.bounds))as held:
                    records=[];row=held.descriptor;vector=held.vector
                    for feed in feeds:
                        held.require(context,vector)
                        selected_policy=_SingletonPolicy(held,selection,feed)
                        result=read_singleton(driver.transport,driver,_BorrowedPins(held),vector,selected_policy,
                            publication_id=row['publication_id'],table=driver.tables['object_current'if selection.kind=='object'else'edge_current'],
                            kind=selection.kind,source=feed,type_id=selection.type_id,entity_id=selection.entity_id,context=context,
                            supported_profiles=['ashlar-delta/0.3'],supported_revisions={k:[v]for k,v in json.loads(row['schema_revisions_json']).items()})
                        records.append({'source_system':feed,'row':None if result is None else dict(result)})
                    answer={'profile':'ashlar-original-two-source-singleton/0.1','original_sha256':config.original.sha256,
                        'ordinal':invocation.ordinal,'descriptor':row,'kind':selection.kind,'type_id':selection.type_id,
                        'entity_id':selection.entity_id,'records':records,
                        'qualification':'private-local-original-two-source-complete-singleton; retained protected receipt SELECT reconciliation; no ACK submission'}
            except BaseException as error:primary=error
            finish(primary,[renew_local,renew_native,lambda:driver.require(context)])
    return report_body(answer,invocation.bounds.receipt_bytes)
