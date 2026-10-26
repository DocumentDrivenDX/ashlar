# SPDX-License-Identifier: MIT OR Apache-2.0
"""Prepare the next bounded Truss continuation selection, without live authority."""
from dataclasses import dataclass,field
import hashlib,json,os
from pathlib import Path
from tempfile import TemporaryDirectory
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes
from .truss_resume_cli import DEFAULT_CARRIER_PROFILE,parse_resume_truss_configuration
from .truss_page_archive import MAXIMUM_ARCHIVE_BYTES


@dataclass(frozen=True)
class ContinuationSelection:
    connection_uri: str=field(repr=False)
    publication_directory: Path
    jars: tuple
    archived_acceptance_files: tuple


def run_configure_truss_continuation_command(configuration,continuation_result,output):
    configuration=Path(configuration);report_path=Path(continuation_result);destination=Path(output)
    originals=[InvocationFile.read(path,maximum)for path,maximum in ((configuration,65536),(report_path,262144))]
    original=decode_custody_bytes(originals[0].raw);report=decode_custody_bytes(originals[1].raw)
    expected=dict(original)
    if type(expected)is dict:expected.setdefault('carrier_profile',DEFAULT_CARRIER_PROFILE)
    original_intent=None
    if type(original)is dict and 'publication_intent'in original:
        selected=original['publication_intent']
        if type(selected)is not dict or set(selected)!={'path','sha256'}:raise ValueError('Exact original intent selection required')
        owner=InvocationFile.read(Path(selected['path']),65536);originals.append(owner)
        if hashlib.sha256(owner.raw).hexdigest()!=selected['sha256']:raise ValueError('Original selected intent digest differs')
        original_intent=decode_custody_bytes(owner.raw)
        selections=('feed','source_page_path','source_page_sha256','request_digest')
        if type(original_intent)is not dict or set(original_intent)!=set(selections)|{'publication_id'}:raise ValueError('Original closed selected intent required')
        expected={k:v for k,v in original.items()if k!='publication_intent'}|{k:original_intent[k]for k in selections}
    config,publication,_,jars,archives,*_=parse_resume_truss_configuration(configuration,destination)
    if config!=expected:raise ValueError('Original configuration changed during parsing')
    if (type(report)is not dict or set(report)!={'predecessor','publications','scope'}or type(report['predecessor'])is not str or not report['predecessor']
            or type(report['scope'])is not str or type(report['publications'])is not list or len(report['publications'])>8):raise ValueError('Closed completed continuation result required')
    operator=ContinuationSelection(config['connection_uri'],publication,jars,archives)
    value=dict(original)
    if original_intent is not None and original_intent['publication_id']!=report['predecessor']:
        raise ValueError('Original selected predecessor differs')
    if report['publications']:
        last=report['publications'][-1]
        if type(last)is not dict or set(last)!={'manifest','source_page','publication_intent'}:raise ValueError('Original completed continuation entry required')
        selection=last['publication_intent']
        if type(selection)is not dict or set(selection)!={'path','sha256'}:raise ValueError('Exact completed intent selection required')
        owner=InvocationFile.read(Path(selection['path']),65536);originals.append(owner)
        if hashlib.sha256(owner.raw).hexdigest()!=selection['sha256']:raise ValueError('Original completed intent digest differs')
        intent=decode_custody_bytes(owner.raw)
        if (type(intent)is not dict or set(intent)!={'feed','source_page_path','source_page_sha256','request_digest','publication_id'}
                or intent['feed']!=config['feed']or owner.path.parent!=publication
                or type(last['manifest'])is not dict or last['manifest'].get('publication_id')!=intent['publication_id']
                or last['source_page']!={'path':intent['source_page_path'],'sha256':intent['source_page_sha256']}):raise ValueError('Completed original intent/publication/source correspondence differs')
        value={'connection_uri':operator.connection_uri,'publication_directory':str(operator.publication_directory),'jars':[str(p)for p in operator.jars],'publication_intent':selection}
        if 'destination_id'in original:value['destination_id']=original['destination_id']
        if 'archived_acceptance_files'in original:value['archived_acceptance_files']=original['archived_acceptance_files']
    if any(destination==p or destination in p.parents or p in destination.parents for p in (configuration,report_path,publication)):
        raise ValueError('Separate fresh continuation configuration required')
    raw=(json.dumps(value,indent=2)+'\n').encode()
    with TemporaryDirectory(prefix='.ashlar-next-continuation-',dir=destination.parent)as directory:
        candidate=Path(directory)/'configuration.json';candidate.write_bytes(raw);candidate.chmod(0o600)
        parsed,_,page_raw,*_=parse_resume_truss_configuration(candidate,destination)
        if parsed['feed']!=config['feed']:raise ValueError('Original continuation source differs')
        page=Path(parsed['source_page_path']);identity=page.stat();expected_page=page_raw
        for owner in originals:owner.renew(262144 if owner.path==report_path else 65536)
        closing=snapshot(page,MAXIMUM_ARCHIVE_BYTES);after=page.stat()
        if closing!=expected_page or (identity.st_dev,identity.st_ino)!=(after.st_dev,after.st_ino):raise ValueError('Original selected page changed')
    with os.fdopen(os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'wb')as stream:
        if stream.write(raw)!=len(raw):raise OSError('Incomplete private continuation configuration')
        stream.flush();os.fsync(stream.fileno())
    return {'output':str(destination),'qualification':'Selection preparation only; actual continuation revalidates committed head, source checkpoint and publication custody.'}
