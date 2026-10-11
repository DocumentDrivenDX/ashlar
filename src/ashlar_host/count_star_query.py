"""Explicit installed CountStar commerce lifecycle; no fallback or source grant.

Results are provisional until held readers close, Spark stops, and closing source,
JAR, installation and publication custody pass. SDK imports are lazy. This
composition does not change the historical query-commerce profile.
"""
from dataclasses import asdict, is_dataclass
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path

from ashlar.weft_count_star_distribution import (
    CountStarDistributionPaths,compile_count_star_distribution,installed_count_star_schema_bundle,
)
from .commerce import runtime_paths
from .commerce_path_oracle import commerce_path_cases, original_commerce_path_oracle
from .count_star_oracle import commerce_count_star_cases,original_commerce_count_star_oracle
from .commerce_path_request import commerce_path_request as original_commerce_path_request
from .config import HostError
from .count_star_configuration import QueryCommerceCountStarConfig
from .lifecycle import finish, owned_context
from .count_star_admission import CountStarAdmissionConfig,BACKEND
from .count_star_execution import CountStarExecutionConfig,execute_commerce_count_star
from .count_star_schema import make_offline_count_star_schema_validation
from .publication_reader import open_commerce_reader, ReaderCleanupState
from .source import original_inputs, read_bounded, recompute_dataset

from .commerce_source_receipt import (UMF_REVISION, commerce_public_source_port,
    encode_commerce_json as _encoded, encode_bounded_commerce_json as _bounded_encoded,
    parse_commerce_json as _parse)


from .count_star_request import commerce_count_star_request
from .commerce_query_lifecycle import (run_count_star_query, CommerceQueryObservation as _Observation,
    commerce_query_report_value as _json, publish_commerce_query_report as _publish_report,
    commerce_query_source_port as _source_port)


def query_commerce_count_star(config: QueryCommerceCountStarConfig, *, diagnostics=None) -> dict:
    """Legacy fixed041 lifecycle; publication/source/native obligations unchanged."""
    return run_count_star_query(config,diagnostics=diagnostics)
