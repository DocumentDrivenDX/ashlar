# SPDX-License-Identifier: MIT OR Apache-2.0
"""Compatible Note CLI wrapper over shared actual record evolution."""
import json
from .truss_schema_evolution import capture_catalog,read,evolve_truss_record


def run_evolve_truss_note_command(configuration,output):
    config=json.loads(read(configuration,65536))
    required={'connection_uri','bun','document_path','document_revision','origin_path','accept_request_key','update_request_key','object_id','tag'}
    if type(config)is not dict or set(config)!=required:raise ValueError('Exact Note evolution configuration required')
    if type(config['tag'])is not str or not config['tag']:raise ValueError('Explicit Note tag required')
    selected={key:value for key,value in config.items()if key!='tag'}
    selected.update(document_id='ashlar-note',module_id='notes',record_id='Note',field_name='note-tag',field_value=config['tag'])
    return evolve_truss_record(selected,output,required_original_fields=('note-id','note-text'))
