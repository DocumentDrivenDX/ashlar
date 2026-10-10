"""Explicit carrier preparation with independently admitted original releases.

The selected operator is trusted code. Prepared bytes and receipts grant no
native activation, current publication or source-read authority.
"""
from dataclasses import dataclass
import hashlib
import json
from types import ModuleType, MappingProxyType
from ashlar.graph_release import GraphRelease
from .evolution_cli import InvocationFile, MAX_CONFIG, MAX_PROVIDER
from .lifecycle import finish, owned_context
from .puppy_release_cli import absolute, digest, original_file, directory_identity, retain, MAX_RELEASE
from .puppy_release_document import load_graph_release
from .puppy_carrier import prepare_carrier

PROFILE = 'ashlar-puppy-preparation-invocation/0.1'

@dataclass(frozen=True)
class PuppyPreparationSession:
    policy: object
    context: object

    def __post_init__(self):
        if not callable(getattr(self.policy,'admit_original_release',None)):
            raise ValueError('Independent original release admission required')


def run_puppy_preparation(configuration, provider_path, provider_sha256):
    config=InvocationFile.read(configuration,MAX_CONFIG)
    from .puppy_native import decode
    value=decode(config.raw)
    if type(value) is not dict or set(value)!={'profile','release_path','release_sha256','output_directory'} or value['profile']!=PROFILE:
        raise ValueError('Closed explicit carrier preparation required')
    source=absolute(value['release_path']);output=absolute(value['output_directory']);release_sha=digest(value['release_sha256'])
    snapshot=original_file(source,MAX_RELEASE)
    load_graph_release(snapshot[0],release_sha)
    release=GraphRelease(snapshot[0],release_sha)
    provider_file=InvocationFile.read(provider_path,MAX_PROVIDER)
    if hashlib.sha256(provider_file.raw).hexdigest()!=digest(provider_sha256):
        raise ValueError('Original selected preparation provider changed')
    module=ModuleType('ashlar_puppy_preparation_'+provider_sha256);module.__file__=str(provider_file.path)
    primary=None;receipt=None;prepared_files={};output_identity=None
    try:
        exec(compile(provider_file.raw,str(provider_file.path),'exec'),module.__dict__)
        factory=getattr(module,'open_puppy_preparation',None)
        if not callable(factory):raise ValueError('Explicit preparation provider required')
        with owned_context(factory(MappingProxyType(dict(value)),release)) as session:
            if type(session) is not PuppyPreparationSession:raise ValueError('Typed ordinary preparation session required')
            session.__post_init__()
            receipt=prepare_carrier(release.payload,release.sha256,output,policy=session.policy,context=session.context)
            output_identity=directory_identity(output)
            for name,maximum in ((receipt['database_name'],32*1024*1024),('model.json',MAX_RELEASE),('original-intent.json',65536)):
                prepared_files[name]=(maximum,original_file(output/name,maximum))
    except BaseException as error:primary=error
    def renew_release():
        if original_file(source,MAX_RELEASE)!=snapshot:raise ValueError('Original release changed during preparation')
    def renew_output():
        if output_identity is not None:
            if directory_identity(output)!=output_identity:raise ValueError('Original carrier directory changed')
            for name,(maximum,original) in prepared_files.items():
                if original_file(output/name,maximum)!=original:raise ValueError('Original prepared carrier changed')
    finish(primary,[lambda:config.renew(MAX_CONFIG),lambda:provider_file.renew(MAX_PROVIDER),renew_release,renew_output])
    if type(receipt) is not dict:raise ValueError('Closed carrier preparation required')
    retain(output,'receipt.json',(json.dumps(receipt,indent=2)+'\n').encode(),output_identity)
    return receipt
