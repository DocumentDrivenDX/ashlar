"""Fixed continuation preflight observations; never submission or authority ports."""
from __future__ import annotations
from typing import Callable, Mapping, TypeVar
from contextlib import contextmanager
from types import MappingProxyType
import time

T = TypeVar("T")

STAGES = frozenset(('writer-acquisition', 'ledger-opening', 'run-identity',
    'producer-admission', 'prefix-native-admission', 'selected-slot-admission', 'closing'))


class EvolutionContinuationTiming:
    """Bound observer attempts; ordinary diagnostics fail without changing work.

    A missing end identifies an unfinished observed interval, not its cause.
    Observer cancellations propagate only after required action/cleanup attempts.
    """
    def __init__(self, observer: Callable[[Mapping[str, object]], None]) -> None:
        if not callable(observer):
            raise ValueError('Explicit continuation stage observer required')
        self.observer = observer
        self.records = 0
        self.lost_records = 0

    def emit(self, stage: str, phase: str, *, duration_ns: int | None = None, outcome: str | None = None) -> None:
        if (type(stage) is not str or stage not in STAGES
                or type(phase) is not str or phase not in ('begin', 'end')
                or phase == 'begin' and (duration_ns is not None or outcome is not None)
                or phase == 'end' and (type(duration_ns) is not int or not 0 <= duration_ns < 2**63
                    or type(outcome) is not str or outcome not in ('completed', 'refused'))
                or self.records >= 128):
            raise ValueError('Closed bounded continuation stage required')
        row = {'profile': 'ashlar-evolution-continuation-stage/0.2',
               'sequence': self.records, 'stage': stage, 'phase': phase}
        if phase == 'end':
            row.update(duration_ns=duration_ns, outcome=outcome)
        self.records += 1
        self.observer(MappingProxyType(row))

    def call(self, stage: str, action: Callable[[], T]) -> T:
        if not callable(action):
            raise ValueError('Explicit continuation action required')
        primary = None; cancellation = None; result = None
        started = time.monotonic_ns()
        def observe(phase, **fields):
            nonlocal cancellation
            try: self.emit(stage, phase, **fields)
            except BaseException as error:
                self.lost_records += 1
                if not isinstance(error, Exception) and cancellation is None:
                    cancellation = error
        observe('begin')
        try: result = action()
        except BaseException as error:
            primary = error
            if not isinstance(error, Exception) and cancellation is None:
                cancellation = error
        observe('end', duration_ns=max(0, time.monotonic_ns()-started),
                outcome='completed' if primary is None and cancellation is None else 'refused')
        if cancellation is not None: raise cancellation
        if primary is not None: raise primary
        return result

    @contextmanager
    def context(self, stage, manager):
        from .lifecycle import finish
        acquired = False; primary = None; value = None
        def enter():
            nonlocal acquired, value
            value = manager.__enter__(); acquired = True
            return value
        try:
            self.call(stage, enter)
            yield value
        except BaseException as error: primary = error
        def close():
            return manager.__exit__(type(primary) if primary is not None else None,
                primary, primary.__traceback__ if primary is not None else None)
        finish(primary, [] if not acquired else [lambda: self.call('closing', close)])
