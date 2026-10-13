"""Prepare existing-publication query selections; no producer or engine execution."""
import json,os
from pathlib import Path
from .evolution_cli import InvocationFile
from .supply_chain_native_cli import parse_supply_chain_native_invocation
from .supply_chain_common_cli import parse_common_configuration


def run_configure_supply_chain_common_command(configuration,python,java_home,output):
    original=InvocationFile.read(Path(configuration),65536)
    selected=parse_supply_chain_native_invocation(original.raw).native
    producer=selected.producer
    value={'profile':'ashlar-supply-chain-common-query/0.1','publication_directory':str(selected.output),
        'python':str(python),'java_home':str(java_home),'jars':str(selected.jars),'pack':str(selected.pack),
        'model':str(selected.model),'graph':str(selected.graph),
        'producer':{**{k:str(getattr(producer,k))for k in ('source','bun','git')},
            **{k:getattr(producer,k)for k in ('timeout_seconds','maximum_output_bytes','maximum_receipt_bytes')}},
        'compiler':{'index':str(selected.compiler.index),'installation':str(selected.compiler.output)},
        'operation_capacity':None if selected.operation_capacity is None else dict(selected.operation_capacity)}
    raw=(json.dumps(value,indent=2)+'\n').encode()
    if len(raw)>65536:raise ValueError('Complete common configuration exceeds input bound')
    parsed=parse_common_configuration(raw);destination=Path(output)
    inputs=(original.path,parsed.root,parsed.python,parsed.java_home,parsed.jars,parsed.pack,parsed.model,parsed.graph,
        producer.source,producer.bun,producer.git,selected.compiler.index,selected.compiler.output)
    if (not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()
            or destination.exists()or destination.is_symlink()
            or any(destination==p or destination in p.parents or p in destination.parents for p in inputs)):
        raise ValueError('Fresh separate canonical common configuration required')
    original.renew(65536)
    with os.fdopen(os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'wb')as stream:
        if stream.write(raw)!=len(raw):raise OSError('Complete private configuration write required')
        stream.flush();os.fsync(stream.fileno())
    return {'output':str(destination)}
