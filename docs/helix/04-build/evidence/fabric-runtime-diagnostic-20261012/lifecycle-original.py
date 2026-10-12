"""Owned cleanup preserves primary failures; no availability or durability claim."""
from contextlib import contextmanager


def finish(primary, callbacks):
    cleanup = None
    cancellation = None
    for callback in callbacks:
        try:
            callback()
        except BaseException as error:
            if cleanup is None: cleanup = error
            if not isinstance(error, Exception) and cancellation is None:
                cancellation = error
    owner = primary if primary is not None and not isinstance(primary, Exception) else cancellation
    if owner is None:
        owner = primary if primary is not None else cleanup
    if owner is not None:
        if cleanup is not None:
            try: setattr(owner, 'cleanup_failed', True)
            except BaseException: pass
        raise owner


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
                if isinstance(primary, Exception) and not isinstance(error, Exception):
                    raise
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
