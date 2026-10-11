"""Explicit042 full lifecycle; ordinary sessions and original pack are mandatory."""
from .config import HostError
from .implicit_arithmetic_configuration import QueryCommerceImplicitArithmeticConfig
from .commerce_query_lifecycle import run_implicit_arithmetic_query


def query_commerce_implicit_arithmetic(config:QueryCommerceImplicitArithmeticConfig, *,
                                      ordinary_session, pack_bytes:bytes, diagnostics=None, invocation_custody=None)->dict:
    """Publish complete evidence only after shared original closing obligations."""
    return run_implicit_arithmetic_query(config,ordinary_session=ordinary_session,pack_bytes=pack_bytes,diagnostics=diagnostics,invocation_custody=invocation_custody)
