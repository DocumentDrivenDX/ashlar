# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure fixed041 commerce request markers; no source/publication authority."""
from .commerce_path_request import commerce_path_request as original_commerce_path_request
from .count_star_admission import BACKEND

def commerce_count_star_request(sql,model,bindings,manifest,registry,aliases):
    """Preserve all original SQL/model/bindings; select only explicit 041 markers."""
    request=original_commerce_path_request(sql,model,bindings,manifest,registry,aliases,profile='paths-keys')
    request.update(interfaceVersion='weft-compile/0.4.1',dialect='weft-sql/0.4.1')
    request['target'].update({key:BACKEND[key]for key in ('backendId','backendVersion','targetProfile')})
    return request
