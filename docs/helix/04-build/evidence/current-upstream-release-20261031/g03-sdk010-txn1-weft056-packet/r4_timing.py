"""Attempt-local timing hooks for a future R4 worker; installs no effects itself."""
from __future__ import annotations

from contextlib import contextmanager
import time


PROFILE = "ashlar-g03-r4-stage-timings/0.1"
MAX_EVENTS = 32
MAX_BYTES = 8192
ALLOWED = {
    "source.open_sdk010",
    "runtime.initialize",
    "runtime.transaction1",
    "metadata.reservation",
    "metadata.phases_total",
    "metadata.operation_mutation",
    "metadata.operation_recovery",
    "metadata.commit_observation",
}


class StageTimings:
    """Bounded monotonic stage observations; payloads and SQL are never retained."""

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._origin = clock()
        self.events = []

    @contextmanager
    def measure(self, stage):
        if stage not in ALLOWED:
            raise ValueError("unknown timing stage")
        if len(self.events) >= MAX_EVENTS:
            raise ValueError("timing event limit exceeded")
        started = self._clock()
        outcome = "ok"
        try:
            yield
        except BaseException:
            outcome = "error"
            raise
        finally:
            ended = self._clock()
            event = {"stage": stage, "startOffsetMs": max(0, round((started-self._origin)*1000)),
                     "durationMs": max(0, round((ended-started)*1000)), "outcome": outcome}
            self.events.append(event)
            if len(self.to_bytes()) > MAX_BYTES:
                self.events.pop()
                raise ValueError("timing receipt byte limit exceeded")

    def to_bytes(self):
        import json
        raw = {"profile": PROFILE, "events": self.events}
        return (json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _timed(method, timings, stage):
    def wrapped(*args, **kwargs):
        with timings.measure(stage):
            return method(*args, **kwargs)
    wrapped.__name__ = getattr(method, "__name__", "timed")
    wrapped.__doc__ = getattr(method, "__doc__")
    return wrapped


def install_sdk010_transaction_hooks(timings):
    """Wrap only named existing boundaries in this packet's isolated worker."""
    from ashlar_host.configured_source_local import ConfiguredLocalSource
    from ashlar_host.configured_local_delta4_publication import ConfiguredLocalDelta4WriterAuthority
    from ashlar_host.delta_custody import LocalDeltaTransport

    original_source_init = ConfiguredLocalSource.__init__
    def source_init(self, *args, **kwargs):
        if kwargs.get("sdk010_admission") is None or kwargs.get("sdk058_admission") is not None:
            return original_source_init(self, *args, **kwargs)
        with timings.measure("source.open_sdk010"):
            return original_source_init(self, *args, **kwargs)
    ConfiguredLocalSource.__init__ = source_init
    restored = [(ConfiguredLocalSource, "__init__", original_source_init)]
    targets = (
        (ConfiguredLocalDelta4WriterAuthority, "reserve_delta4_prepared_phase", "metadata.reservation"),
        (ConfiguredLocalDelta4WriterAuthority, "prepare_delta4_native_prepared_phase", "metadata.phases_total"),
        (LocalDeltaTransport, "metadata_mutation", "metadata.operation_mutation"),
        (LocalDeltaTransport, "metadata_recover", "metadata.operation_recovery"),
        (LocalDeltaTransport, "observe_delta4_metadata_commit", "metadata.commit_observation"),
    )
    for owner, name, stage in targets:
        original = getattr(owner, name)
        setattr(owner, name, _timed(original, timings, stage))
        restored.append((owner, name, original))

    def restore():
        for owner, name, original in reversed(restored):
            setattr(owner, name, original)
    return restore
