"""Small effect-free client runtime check against the selected public Truss SDK."""


def require_truss_psycopg_runtime():
    """Check the three client components; the live SDK still admits the server."""
    import psycopg
    from psycopg import pq
    from truss import PsycopgProfile
    profile=PsycopgProfile()
    required=(profile.psycopg,profile.implementation,profile.libpq)
    selected=(psycopg.__version__,pq.__impl__,pq.version())
    if selected!=required:
        raise ValueError('Selected Truss SDK requires psycopg/implementation/libpq '+repr(required)+'; current client tuple is '+repr(selected))
