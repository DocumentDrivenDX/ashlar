"""Durable cleanup uncertainty port; actual native outcome verification is external."""
from dataclasses import dataclass
from .publication import _name
from .pins import PinError

@dataclass(frozen=True)
class CleanupRequest:
    operation: str
    table: str
    uuid: str
    original_intent: bytes

    def __post_init__(self):
        _name(self.table)
        if any(not isinstance(v,str) or not v or len(v.encode())>1024 for v in (self.operation,self.uuid)):
            raise PinError('Bounded original cleanup identity required')
        if not isinstance(self.original_intent,bytes) or not 1<=len(self.original_intent)<=1048576:
            raise PinError('Bounded original cleanup intent bytes required')


class CleanupQuarantine:
    def __init__(self,executor,policy):
        self.executor=executor;self.policy=policy
    def begin(self,request,*,context):
        if not isinstance(request,CleanupRequest):raise PinError('Original cleanup request required')
        if self.policy.admit_cleanup(request,context) is not None:
            raise PinError('Cleanup scope/authority admission incomplete')
        with self.executor.transaction(context) as session:
            session.query("SELECT ashlar_pins.quarantine_cleanup(:operation,:table,:uuid,decode(:intent,'hex'))",
                {'operation':request.operation,'table':request.table,'uuid':request.uuid,'intent':request.original_intent.hex()})
        # Remote work may start only after this original quarantine commits.
    def close(self,request,terminal_custody,*,context):
        if not isinstance(request,CleanupRequest) or not isinstance(terminal_custody,bytes) or not 1<=len(terminal_custody)<=1048576:
            raise PinError('Original cleanup and bounded terminal bytes required')
        if self.policy.verify_terminal(request,terminal_custody,context) is not None:
            raise PinError('Independent native terminal/outcome verification incomplete')
        with self.executor.transaction(context) as session:
            session.query("SELECT ashlar_pins.close_cleanup_quarantine(:operation,decode(:intent,'hex'),decode(:terminal,'hex'))",
                {'operation':request.operation,'intent':request.original_intent.hex(),'terminal':terminal_custody.hex()})
