# SPDX-License-Identifier: MIT OR Apache-2.0
"""Accepted record evolution followed by two original feed publications."""
import hashlib,json
from pathlib import Path
from .evolution_cli import InvocationFile
from .private_graph_custody import decode_custody_bytes
from .truss_resume_cli import parse_resume_truss_configuration
from .truss_schema_evolution import parse_truss_record_evolution_configuration,evolve_truss_record
from .truss_continue_cli import run_continue_truss_graph_command
from .truss_continuation_setup import run_configure_truss_continuation_command
from .truss_example import private_file
from .exact_report import report_body
from .evolution_producer import snapshot


def run_evolve_truss_graph_command(configuration,reader_configuration,output):
    source_path=Path(configuration);reader_path=Path(reader_configuration);root=Path(output)
    for path in (source_path,reader_path):
        if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical original configurations required')
    if not root.is_absolute()or root.parent.resolve()!=root.parent or not root.parent.is_dir()or root.exists()or root.is_symlink():raise ValueError('Fresh canonical evolution output required')
    originals=[InvocationFile.read(path,65536)for path in (source_path,reader_path)]
    source=decode_custody_bytes(originals[0].raw);reader=decode_custody_bytes(originals[1].raw)
    parse_truss_record_evolution_configuration(source,root.parent/(root.name+'-preflight.json'))
    parsed,publication,_,jars,archives,*_=parse_resume_truss_configuration(reader_path,root.parent/(root.name+'-preflight-reader.json'))
    # Bind operator settings to the captured original observation, including intent normalization.
    expected=dict(reader)
    if 'publication_intent'in expected:
        selection=expected.pop('publication_intent');intent_owner=InvocationFile.read(Path(selection['path']),65536)
        if hashlib.sha256(intent_owner.raw).hexdigest()!=selection['sha256']:raise ValueError('Original intent digest differs')
        intent=decode_custody_bytes(intent_owner.raw)
        expected.update({key:intent[key]for key in ('feed','source_page_path','source_page_sha256','request_digest')});originals.append(intent_owner)
    if parsed!=expected or source['connection_uri']!=parsed['connection_uri']:raise ValueError('Same original ordinary source required')
    if len(archives)>=16:raise ValueError('Room for original evolution archive required')
    protected=[source_path,reader_path,publication,*jars,*(item[0]for item in archives),*(Path(source[key]).resolve()for key in ('bun','document_path','origin_path')),Path(parsed['source_page_path'])]
    if any(root==path or root in path.parents or path in root.parents for path in protected):raise ValueError('Separate evolution output required')
    for original in originals:original.renew(65536)
    root.mkdir(mode=0o700)
    evolve_truss_record(source,root/'evolution.json')
    archive=snapshot(root/'evolution.json',16777216)
    augmented=dict(reader);augmented['archived_acceptance_files']=[*reader.get('archived_acceptance_files',[]),{'path':str(root/'evolution.json'),'sha256':hashlib.sha256(archive).hexdigest()}]
    selected=private_file(root/'evolution-reader-configuration.json',augmented)
    continued=run_continue_truss_graph_command(selected,root/'continuation.json',maximum_pages=2,wait_seconds=0,maximum_page_bytes=2097152)
    if len(continued['publications'])!=2:raise ValueError('Two complete evolution publications required')
    run_configure_truss_continuation_command(selected,root/'continuation.json',root/'reader-configuration.json')
    for original in originals:original.renew(65536)
    if snapshot(root/'evolution.json',16777216)!=archive:raise ValueError('Original evolution archive changed')
    answer={'profile':'ashlar-truss-graph-evolution/0.1','evolution':str(root/'evolution.json'),'continuation':str(root/'continuation.json'),'reader_configuration':str(root/'reader-configuration.json'),'publication_id':continued['publications'][-1]['manifest']['publication_id'],'publications':2,'qualification':'Accepted record evolution and complete feed publication; no mixed-revision Weft query qualification.'}
    private_file(root/'result.json',report_body(answer,1048576))
    return answer
