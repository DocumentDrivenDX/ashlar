"""CONTRACT-073 public encoding of an original local native Delta receipt."""

from __future__ import annotations

import re

from umf_access.native_program import canonical_json
from umf_access.protocols import Receipt

from .delta_atomic import DeltaAtomicReceipt


def delta_store(table_uuid: str) -> str:
    if (
        type(table_uuid) is not str
        or re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", table_uuid
        )
        is None
    ):
        raise ValueError("original canonical native Delta table UUID required")
    return canonical_json([table_uuid])


def sdk_delta_receipt(original: DeltaAtomicReceipt) -> Receipt:
    """Encode only the retained native commit, never a table history head.

    NativeDeltaAtomicReceipt remains unchanged and retains its original request,
    metadata and integer version for native reconciliation. This conversion
    grants no new native admission, human attribution or replay authority.
    """
    if type(original) is not DeltaAtomicReceipt:
        raise TypeError("original native DeltaAtomicReceipt required")
    if type(original.version) is not int or original.version < 0:
        raise ValueError("original nonnegative native Delta version required")
    return Receipt(
        delta_store(original.table_uuid),
        original.epoch,
        canonical_json(
            [{"tableUuid": original.table_uuid, "version": str(original.version)}]
        ),
    )
