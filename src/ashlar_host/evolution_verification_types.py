"""Closed verification selections; these values confer no publication authority."""
from dataclasses import dataclass
from pathlib import Path
import re
from .config import EvolutionAdmissionConfig


@dataclass(frozen=True)
class VerificationBounds:
    input_bytes: int
    rows: int
    cell_bytes: int
    total_bytes: int
    receipt_bytes: int
    capture_bytes: int

    def __post_init__(self):
        ceilings = (262144, 10000, 1048576, 16777216, 1048576, 1048576)
        for value, maximum in zip((self.input_bytes, self.rows, self.cell_bytes,
                self.total_bytes, self.receipt_bytes, self.capture_bytes), ceilings):
            if type(value) is not int or not 1 <= value <= maximum:
                raise PermissionError('Explicit bounded verification limits required')


@dataclass(frozen=True)
class VerificationSupervision:
    """Explicit operator supervisor selection, not a claim of CLI-owned timeout."""
    deadline_seconds: int
    capture_bytes: int
    owner: str

    def __post_init__(self):
        if (type(self.deadline_seconds) is not int or not 1 <= self.deadline_seconds <= 300
                or type(self.capture_bytes) is not int or not 1 <= self.capture_bytes <= 1048576
                or self.owner != 'operator-owned-process-supervisor/0.1'):
            raise PermissionError('Explicit finite owning verification supervision required')


@dataclass(frozen=True)
class VerifyEvolutionInvocation:
    ledger_path: Path
    receipt_path: Path
    producer: EvolutionAdmissionConfig
    expected_sha256: str
    ordinal: int
    bounds: VerificationBounds
    supervision: VerificationSupervision

    def __post_init__(self):
        if (type(self.producer) is not EvolutionAdmissionConfig
                or type(self.bounds) is not VerificationBounds
                or type(self.supervision) is not VerificationSupervision
                or type(self.ordinal) is not int or not 0 <= self.ordinal < 8
                or type(self.expected_sha256) is not str
                or re.fullmatch('[0-9a-f]{64}', self.expected_sha256) is None):
            raise PermissionError('Exact original verification selection required')
        self.producer.__post_init__(); self.bounds.__post_init__(); self.supervision.__post_init__()
        if (self.supervision.capture_bytes != self.bounds.capture_bytes
                or any(type(path) is not type(Path()) or not path.is_absolute()
                    or path.parent.resolve() != path.parent for path in (self.ledger_path, self.receipt_path))
                or self.ledger_path == self.receipt_path):
            raise PermissionError('Canonical distinct original verification paths and capture agreement required')


@dataclass(frozen=True)
class VerifyCommerceEvolutionConfig:
    driver: object
    ledger_path: Path
    policy: object
    producer: EvolutionAdmissionConfig
    context: object
    expected_sha256: str
    ordinal: int
    bounds: VerificationBounds
    supervision: VerificationSupervision
    original: object

    def __post_init__(self):
        if (type(self.ledger_path) is not type(Path()) or not self.ledger_path.is_absolute()
                or self.ledger_path.parent.resolve() != self.ledger_path.parent
                or type(self.producer) is not EvolutionAdmissionConfig
                or type(self.bounds) is not VerificationBounds or type(self.supervision) is not VerificationSupervision
                or type(self.ordinal) is not int or not 0 <= self.ordinal < 8
                or type(self.expected_sha256) is not str or re.fullmatch('[0-9a-f]{64}', self.expected_sha256) is None
                or self.bounds.capture_bytes != self.supervision.capture_bytes or self.context is None):
            raise PermissionError('Exact live verification configuration required')
        self.producer.__post_init__(); self.bounds.__post_init__(); self.supervision.__post_init__()
