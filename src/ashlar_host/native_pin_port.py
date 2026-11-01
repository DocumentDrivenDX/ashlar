# SPDX-License-Identifier: MIT OR Apache-2.0
"""Source-bound renewable adapter for the existing native pin owner.

The pin transaction protects exact registered UUID/version custody. Retention
coverage is a separate authority and must be supplied by the caller at every
observation; a pin row never supplies that authority.
"""
from __future__ import annotations

import math
from typing import Protocol

from ashlar.pins import PinVector, PostgresPins


class NativePinPortError(ValueError):
    """The native pin or independent retention admission refused."""


class SourceBoundRetentionAdmission(Protocol):
    def remaining_lifetime_seconds(
        self, vector: PinVector, source_selection: object, context: object,
    ) -> float: ...


class NativePinPort:
    """Adapt ``PostgresPins.open_live`` to the source-holder port contract.

    ``retention`` must independently admit the exact source/publication scope
    and report its remaining bounded coverage. It is called at open, renewal,
    every coverage query, and close. The native pin handle separately rechecks
    every registered UUID/version pair inside its held transaction.
    """

    def __init__(self, pins: PostgresPins, retention: SourceBoundRetentionAdmission):
        if (type(pins) is not PostgresPins
                or not callable(getattr(retention, "remaining_lifetime_seconds", None))):
            raise NativePinPortError("Native pin owner and source-bound retention admission required")
        self._pins = pins
        self._retention = retention

    def open(self, vector: PinVector, source_selection: object, context: object) -> "_NativePinHandle":
        if type(vector) is not PinVector or source_selection is None or context is None:
            raise NativePinPortError("Exact pin vector, source selection and context required")
        # The native owner acquires and checks the transaction before exposing
        # the handle. If separate retention admission refuses, release it.
        native = self._pins.open_live(vector, source_selection, context=context)
        handle = _NativePinHandle(self._retention, native, vector, source_selection, context)
        try:
            handle._coverage()
        except BaseException as error:
            handle._close_after(error)
            raise
        return handle


class _NativePinHandle:
    def __init__(self, retention, native, vector, source_selection, context):
        self._retention = retention
        self._native = native
        self._vector = vector
        self._source_selection = source_selection
        self._context = context
        self._active = True

    def _require_active(self):
        if not self._active:
            raise NativePinPortError("Native pin port handle is closed")

    def _coverage(self) -> float:
        self._require_active()
        try:
            value = self._retention.remaining_lifetime_seconds(
                self._vector, self._source_selection, self._context)
        except Exception as error:
            raise NativePinPortError("Independent source-bound retention admission failed") from error
        if (type(value) not in (int, float) or not math.isfinite(value) or value < 0):
            raise NativePinPortError("Finite source-bound retention coverage required")
        return float(value)

    def remaining_lifetime_seconds(self) -> float:
        """Return fresh coverage from the independent retention authority."""
        return self._coverage()

    def renew(self) -> None:
        self._require_active()
        try:
            self._native.renew()
        except BaseException:
            # PostgresPins poisons and releases its transaction if its own
            # renewal check fails.
            self._active = False
            raise
        try:
            self._coverage()
        except BaseException as error:
            self._close_after(error)
            raise

    def _close_after(self, primary: BaseException) -> None:
        if not self._active:
            return
        self._active = False
        try:
            self._native.close()
        except BaseException as close_error:
            add_note = getattr(primary, "add_note", None)
            if callable(add_note):
                add_note("Native pin cleanup failed: " + type(close_error).__name__)

    def close(self) -> None:
        self._require_active()
        primary = None
        try:
            self._coverage()
        except BaseException as error:
            primary = error
        self._active = False
        try:
            # The native owner performs its own final complete-vector check.
            self._native.close()
        except BaseException as error:
            if primary is None:
                primary = error
            else:
                add_note = getattr(primary, "add_note", None)
                if callable(add_note):
                    add_note("Native pin close check failed: " + type(error).__name__)
        if primary is not None:
            raise primary
