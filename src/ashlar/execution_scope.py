"""Public held-context execution preserving the original refusal and value."""
from contextlib import contextmanager

@contextmanager
def preserve_body_failure(manager):
    """Original refusal survives suppression or a separate closing failure."""
    original = None
    try:
        with manager as value:
            try:
                yield value
            except BaseException as error:
                original = error
                raise
    except BaseException as closing:
        if original is not None and closing is not original:
            raise original from closing
        raise
    if original is not None:
        raise original

