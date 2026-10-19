# SPDX-License-Identifier: MIT OR Apache-2.0
"""Public original medical singleton composition; actual reader owns all closing."""
from .medical_singleton_query import MedicalSingletonQuery
from .original_medical_local_read import read_original_medical_local


def query_original_medical_singleton(configuration,compiler_paths,*,supervision=None):
    """One read/Spark interval; no result before original source/native closing."""
    selection=MedicalSingletonQuery(compiler_paths)
    return read_original_medical_local(configuration,supervision=supervision,singleton_query=selection)
