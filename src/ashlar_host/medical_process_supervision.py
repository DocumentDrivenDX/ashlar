# SPDX-License-Identifier: MIT OR Apache-2.0
"""Private worker phase signals; no data authority or native acceptance claim."""
from dataclasses import asdict
import fcntl
import os
import stat

from .original_medical_local_configuration import OriginalMedicalBounds
from .original_medical_runtime import OriginalMedicalRuntimeCustody


class OriginalMedicalNativePhase:
    """Single native interval on an explicitly supplied nonblocking pipe.

    The parent owns the pipe lifetime and process deadlines. This worker owner
    never closes or substitutes the inherited descriptor.
    """
    def __init__(self, descriptor):
        if type(descriptor) is not int or descriptor < 0:
            raise ValueError('Explicit native phase descriptor required')
        self._descriptor = descriptor
        self._identity = self._observe_descriptor()
        self._state = 'ready'
        self._custody = None
        self._bounds = None
        self._bounds_value = None

    def _observe_descriptor(self):
        value = os.fstat(self._descriptor)
        flags = fcntl.fcntl(self._descriptor, fcntl.F_GETFL)
        if (not stat.S_ISFIFO(value.st_mode)
                or flags & os.O_ACCMODE != os.O_WRONLY
                or os.get_blocking(self._descriptor)):
            raise PermissionError('Writable nonblocking native phase pipe required')
        return (value.st_dev, value.st_ino, value.st_mode)

    def _send(self, message):
        if self._observe_descriptor() != self._identity:
            raise PermissionError('Original native phase descriptor changed')
        # Both fixed messages fit PIPE_BUF. Refuse a failed/partial send rather
        # than retrying an uncertain signal or blocking native cleanup.
        self._state = 'failed'
        if os.write(self._descriptor, message) != len(message):
            raise OSError('Incomplete native phase signal')

    def begin(self, custody):
        if self._state != 'ready' or type(custody) is not OriginalMedicalRuntimeCustody:
            raise ValueError('Fresh phase and exact medical runtime custody required')
        custody.renew()
        if type(custody.bounds) is not OriginalMedicalBounds:
            raise ValueError('Exact medical native bounds required')
        custody.bounds.__post_init__()
        self._custody = custody
        self._bounds = custody.bounds
        self._bounds_value = asdict(custody.bounds)
        self._send(b'native.begin\n')
        self._state = 'begun'

    def end(self, custody):
        if self._state != 'begun' or custody is not self._custody:
            raise ValueError('Original begun medical phase custody required')
        custody.renew()
        if custody.bounds is not self._bounds or asdict(custody.bounds) != self._bounds_value:
            raise PermissionError('Original medical phase bounds changed')
        self._send(b'native.end\n')
        self._state = 'ended'
