# SPDX-License-Identifier: MIT OR Apache-2.0
"""Nominal observations bound to an actual open core phase-read session.

These carriers confer no writer, native, source or ACK authority.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class PhaseReadRequest:
    session: object
    context: object
    table: str
    uuid: str
    stream: str
    batch_id: str
    original_request_json: str

    def renew(self,context):
        from .attempt_store import validate_phase_read_session
        validate_phase_read_session(self.session,context,self)

    def __post_init__(self):
        self.renew(self.context)


@dataclass(frozen=True)
class PhaseAbsence:
    request: PhaseReadRequest

    def __post_init__(self):
        if type(self.request)is not PhaseReadRequest:raise ValueError('Exact core phase read required')
        self.request.renew(self.request.context)
