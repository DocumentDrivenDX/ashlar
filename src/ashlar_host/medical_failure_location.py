# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded passive owned-code failure locations without exception payloads."""
import builtins
import json
import re

MAXIMUM_FAILURE_LOCATION_BYTES = 8192
_IDENTIFIER = re.compile(r'[A-Za-z_][A-Za-z_0-9]{0,127}\Z')
_MODULE = re.compile(r'ashlar(?:_host)?(?:\.[A-Za-z_][A-Za-z_0-9]{0,127}){1,8}\Z')


def medical_failure_location(error):
    """Return only bounded identifiers; never format an exception or frame."""
    category = 'BaseException'
    for kind in type(error).__mro__:
        name = kind.__name__
        if kind.__module__ == 'builtins' and getattr(builtins, name, None) is kind and issubclass(kind, BaseException):
            category = name
            break
    frames = []
    trace = error.__traceback__
    scanned = 0
    while trace is not None and scanned < 256:
        frame = trace.tb_frame
        module = frame.f_globals.get('__name__')
        function = frame.f_code.co_name
        line = trace.tb_lineno
        if type(module) is str and _MODULE.fullmatch(module) and _IDENTIFIER.fullmatch(function) and type(line) is int and 0 < line <= 10000000:
            frames.append({'module': module, 'function': function, 'line': line})
            frames = frames[-64:]
        scanned += 1
        trace = trace.tb_next
    # Long identifiers are valid but need not consume the entire passive budget.
    while True:
        raw = (json.dumps({'profile': 'ashlar-original-medical-failure-location/0.1', 'exception': category, 'frames': frames}, sort_keys=True, separators=(',', ':')) + '\n').encode('ascii')
        if len(raw) <= MAXIMUM_FAILURE_LOCATION_BYTES:
            return raw
        frames.pop(0)


def retain_medical_failure_location(output, error):
    """Use the existing output owner only after its acquisition succeeded."""
    try:
        if output.fd is not None:
            output.write('failure-location.json', medical_failure_location(error), MAXIMUM_FAILURE_LOCATION_BYTES)
    except BaseException:
        # This passive diagnostic can never replace the original failure.
        try: error.cleanup_failed = True
        except BaseException: pass
