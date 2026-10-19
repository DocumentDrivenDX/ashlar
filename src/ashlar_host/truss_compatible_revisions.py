# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure accepted STRING-record compatibility; grants no live source authority."""
import re
from .truss_graph_admission import TrussGraphAdmission


def compatible_record_revisions(admission, *, revision, document_id, type_id):
    """Retained revisions readable under the selected record's optional additions.

    Current field definitions, required presence and ordered logical keys remain
    exact. This tuple is binding data, never mutation, publication or ACK custody.
    """
    if (type(admission) is not TrussGraphAdmission or type(admission.revisions) is not dict
            or not 1 <= len(admission.revisions) <= 64
            or type(revision) is not str or revision not in admission.revisions
            or type(document_id) is not str or not document_id
            or type(type_id) is not str or not re.fullmatch('[1-9][0-9]{0,19}', type_id)
            or any(type(key) is not str or not re.fullmatch('[1-9][0-9]{0,19}', key)
                   for key in admission.revisions)):
        raise ValueError('Selected actual bounded accepted record revisions required')
    selected = admission.revisions[revision]['types'].get(type_id)
    if selected is None or selected['owner'][0] != document_id:
        raise ValueError('Selected accepted record identity required')
    fields = selected['fields']; required = selected['required']; keys = selected['keys']
    if (type(fields) is not dict or type(required) is not set or type(keys) is not dict
            or type(selected['owner']) is not tuple or len(selected['owner']) != 3
            or not required <= set(fields)):
        raise ValueError('Selected accepted STRING record shape required')
    result = []
    for candidate_revision, entry in admission.revisions.items():
        # A later definition cannot become an older reader's interpretation.
        if int(candidate_revision) > int(revision):
            continue
        record = entry['types'].get(type_id)
        if record is None or record['owner'] != selected['owner']:
            continue
        old_fields = record['fields']
        if (type(old_fields) is not dict or type(record['required']) is not set
                or record['required'] != required or record['keys'] != keys
                or any(fields.get(property_id) != pin for property_id, pin in old_fields.items())
                or not required <= set(old_fields)):
            continue
        result.append(candidate_revision)
    if revision not in result:
        raise ValueError('Selected current revision must remain compatible')
    return tuple(sorted(result, key=int))
