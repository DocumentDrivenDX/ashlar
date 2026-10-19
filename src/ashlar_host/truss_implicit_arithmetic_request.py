# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original Truss binding with explicit installed public 0.4.2 selection."""
from .implicit_arithmetic_profile import PROFILE
from .truss_weft_query import truss_scalar_compile_request

def truss_implicit_arithmetic_request(admission,manifest,table_uuids,manifest_uuid,sql,*,revision,document_id,aliases=None):
    request=truss_scalar_compile_request(admission,manifest,table_uuids,manifest_uuid,sql,revision=revision,document_id=document_id,aliases=aliases)
    request.update(interfaceVersion=PROFILE.compile_version,dialect=PROFILE.dialect)
    backend=dict(PROFILE.backend)
    request['target'].update({key:backend[key]for key in ('backendId','backendVersion','targetProfile')})
    return request
