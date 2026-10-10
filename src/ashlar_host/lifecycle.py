"""Owned cleanup preserves primary failures; no availability or durability claim."""
from contextlib import contextmanager


def finish(primary, callbacks):
    cleanup = None
    for callback in callbacks:
        try:
            callback()
        except BaseException as error:
            if cleanup is None: cleanup = error
    if primary is not None:
        if cleanup is not None:
            try: setattr(primary, 'cleanup_failed', True)
            except BaseException: pass
        raise primary
    if cleanup is not None: raise cleanup


@contextmanager
def owned_context(manager):
    primary = None
    try:
        with manager as value:
            try:
                yield value
            except BaseException as error:
                primary = error
                raise
    except BaseException as error:
        if primary is not None:
            if error is not primary:
                try: setattr(primary, 'cleanup_failed', True)
                except BaseException: pass
            raise primary
        raise
    if primary is not None: raise primary


@contextmanager
def owned_connection(connection):
    primary = None
    try:
        yield connection
    except BaseException as error:
        primary = error
    finish(primary, [connection.close])
