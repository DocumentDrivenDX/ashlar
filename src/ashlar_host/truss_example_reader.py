# SPDX-License-Identifier: MIT OR Apache-2.0
"""Prepare a fresh ordinary reader URI; never connect or grant source authority."""
from dataclasses import dataclass
import hashlib,json,os
from pathlib import Path
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .truss_resume_cli import parse_resume_truss_configuration
from .private_graph_custody import decode_custody_bytes


@dataclass(frozen=True)
class AcceptedInput:
    path: Path
    raw: bytes
    identity: tuple
    @classmethod
    def capture(cls,path):
        if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical accepted report required')
        info=path.stat();raw=snapshot(path,1048576);after=path.stat()
        if (info.st_dev,info.st_ino)!=(after.st_dev,after.st_ino):raise ValueError('Accepted report changed')
        return cls(path,raw,(info.st_dev,info.st_ino))
    def renew(self):
        current=type(self).capture(self.path)
        if current.identity!=self.identity or current.raw!=self.raw:raise ValueError('Original accepted report changed')


def run_configure_truss_example_reader_command(example_directory,connection_configuration,output,*,reader_configuration=None):
    root=Path(example_directory);connection=Path(connection_configuration);destination=Path(output)
    if not root.is_absolute()or root.resolve()!=root or not root.is_dir():raise ValueError('Canonical existing example directory required')
    accepted=AcceptedInput.capture(root/'accepted.json')
    reader=Path(reader_configuration)if reader_configuration is not None else root/'query-configuration.json'
    if not reader.is_absolute()or reader.resolve()!=reader or not reader.is_file():raise ValueError('Canonical existing saved reader configuration required')
    owners=[InvocationFile.read(path,65536)for path in (root/'setup.json',reader,connection)]
    setup,query,live=[decode_custody_bytes(owner.raw)for owner in owners];report=decode_custody_bytes(accepted.raw)
    if type(setup)is not dict or type(query)is not dict or type(report)is not dict:raise ValueError('Original example custody objects required')
    if query.get('publication_directory')!=str(root/'publication'):raise ValueError('Original example publication directory differs')
    normalized=query
    if 'publication_intent'in query:
        selected=query['publication_intent']
        if type(selected)is not dict or set(selected)!={'path','sha256'}or type(selected['path'])is not str:raise ValueError('Exact publication intent selection required')
        intent_path=Path(selected['path'])
        if not intent_path.is_absolute()or intent_path.resolve()!=intent_path:raise ValueError('Canonical saved publication intent required')
        intent=InvocationFile.read(intent_path,65536);owners.append(intent)
        if hashlib.sha256(intent.raw).hexdigest()!=selected['sha256']:raise ValueError('Original publication intent digest differs')
        value=decode_custody_bytes(intent.raw)
        selections={'feed','source_page_path','source_page_sha256','request_digest'}
        if type(value)is not dict or not selections<=set(value):raise ValueError('Original publication intent required')
        normalized={k:v for k,v in query.items()if k!='publication_intent'}|{k:value[k]for k in selections}
    if (type(live)is not dict or set(live)!={'profile','data_directory','connection_uri'}or live['profile']!='ashlar-local-truss-connection/0.1'
            or type(live['connection_uri'])is not str or not live['connection_uri']or len(live['connection_uri'])>4096 or '\0'in live['connection_uri']):raise ValueError('Exact original live local connection configuration required')
    data=setup.get('data_directory')
    if type(data)is not str or data!=str(root/'data')or report.get('data_directory')!=data or live['data_directory']!=data:raise ValueError('Original example data directory differs')
    if not Path(data).is_dir()or Path(data).resolve()!=Path(data):raise ValueError('Canonical original example data required')
    roles=report.get('roles');consumer=roles.get('consumer')if type(roles)is dict else None
    if type(consumer)is not str or not consumer or len(consumer)>256 or '\0'in consumer:raise ValueError('Original bounded ordinary consumer role required')
    # Existing pure parser validates original intent, page hashes, jars and output.
    parsed,*_=parse_resume_truss_configuration(owners[1].path,destination)
    if parsed!=normalized:raise ValueError('Original example query configuration changed')
    protected=[root,connection,*(owner.path for owner in owners)]
    if any(destination==p or destination in p.parents or p in destination.parents for p in protected):raise ValueError('Reader output must be separate from original example and connection custody')
    import psycopg
    parameters=psycopg.conninfo.conninfo_to_dict(live['connection_uri']);parameters['user']=consumer
    result={**query,'connection_uri':psycopg.conninfo.make_conninfo(**parameters)}
    accepted.renew()
    for owner in owners:owner.renew(65536)
    raw=(json.dumps(result,indent=2)+'\n').encode()
    with os.fdopen(os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb')as stream:
        if stream.write(raw)!=len(raw):raise OSError('Incomplete private reader configuration')
        stream.flush();os.fsync(stream.fileno())
    return {'output':str(destination),'qualification':'Configuration preparation only; live query must admit original source and publication.'}
