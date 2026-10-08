"""Conservative whole-table pin exclusion; external cleanup containment required.

This does not submit VACUUM or discover all operators. A host must keep original
remote outcome/termination custody and quarantine on lost transaction/uncertainty;
releasing a SQL guard is not evidence that remote work terminated.
"""
from contextlib import contextmanager
from types import MappingProxyType
from .publication import _name
from .pins import PinError


class PostgresTableGuard:
    def __init__(self,executor,policy):
        self.executor=executor;self.policy=policy

    @contextmanager
    def hold(self,targets,*,context):
        if not isinstance(targets,dict) or not 1<=len(targets)<=128:
            raise PinError('Complete bounded cleanup target inventory required')
        copied={}
        for table,uuid in targets.items():
            _name(table)
            if not isinstance(uuid,str) or not uuid or len(uuid.encode())>1024:
                raise PinError('Exact original cleanup UUID required')
            copied[table]=uuid
        targets=MappingProxyType(copied)
        if self.policy.authorize_guard(targets,context) is not None:
            raise PinError('Cleanup guard authority incomplete')
        with self.executor.transaction(context) as session:
            for table in sorted(targets):
                session.query('SELECT ashlar_pins.assert_table_unpinned(:table,:uuid)',
                              {'table':table,'uuid':targets[table]})
            yield targets
            if self.policy.authorize_guard(targets,context) is not None:
                raise PinError('Cleanup guard authority expired')
