"""Prepare a private resume selection without SDK, database or engine effects."""
import hashlib
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from .delta_custody import encoded
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes
from .truss_resume_cli import parse_resume_truss_configuration


def run_configure_truss_resume_command(configuration, publication_intent, output):
    """Select original connection/JARs and an exact saved intent; grant no authority."""
    configuration = Path(configuration); intent_path = Path(publication_intent); destination = Path(output)
    for path in (configuration,intent_path):
        if not path.is_absolute() or path.resolve() != path or not path.is_file():
            raise ValueError('Original canonical configuration and intent files required')
    original = decode_custody_bytes(snapshot(configuration,65536))
    required = {'connection_uri','administrator_uri','reader_role','consumer_id','feed','jars','maximum_pages'}
    if type(original) is not dict or not required <= set(original) <= required | {'wait_seconds','archived_acceptance_files','export_graph','destination_id','maximum_page_bytes'}:
        raise ValueError('Original closed publisher configuration required')
    page_bytes=original.get('maximum_page_bytes',1048576)
    if type(page_bytes)is not int or not 1<=page_bytes<=2097152:raise ValueError('Page byte budget must be 1..2097152')
    intent_raw = snapshot(intent_path,65536)
    intent = decode_custody_bytes(intent_raw)
    if type(intent) is not dict or intent.get('feed') != original['feed']:
        raise ValueError('Original publisher and publication intent feed differs')
    value = {'connection_uri':original['connection_uri'],'jars':original['jars'],
        'publication_directory':str(intent_path.parent),
        'publication_intent':{'path':str(intent_path),'sha256':hashlib.sha256(intent_raw).hexdigest()}}
    if 'destination_id'in original:value['destination_id']=original['destination_id']
    if 'archived_acceptance_files' in original:value['archived_acceptance_files'] = original['archived_acceptance_files']
    if (not destination.is_absolute() or not destination.parent.is_dir()
            or destination.parent.resolve() != destination.parent or destination.exists() or destination.is_symlink()
            or destination == configuration or destination == intent_path):
        raise ValueError('Fresh separate canonical resume configuration output required')
    # Reuse the actual downstream preflight; the candidate file is private and
    # temporary, and contains the same bytes selected for the final configuration.
    raw = (encoded(value)+'\n').encode()
    with TemporaryDirectory(prefix='.ashlar-resume-config-',dir=destination.parent) as directory:
        candidate = Path(directory)/'configuration.json';candidate.write_bytes(raw)
        parse_resume_truss_configuration(candidate,destination)
        hypothetical_result = destination.parent/(destination.name+'.resume-result.json')
        parse_resume_truss_configuration(candidate,hypothetical_result)
    descriptor = os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb') as stream:
        if stream.write(raw) != len(raw):raise OSError('Complete private resume configuration write required')
        stream.flush();os.fsync(stream.fileno())
    return {'output':str(destination),'publication_intent_sha256':value['publication_intent']['sha256']}
