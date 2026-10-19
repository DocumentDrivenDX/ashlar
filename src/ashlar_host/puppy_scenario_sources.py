# SPDX-License-Identifier: MIT OR Apache-2.0
"""Current ordinary original-file use, separately from public UMF semantics."""
from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
from types import MappingProxyType
from .config import ProducerConfig
from .delta_custody import encoded
from .evolution_cli import InvocationFile
from .finite_dataset import FiniteDatasetConfig,HeldFiniteDataset,FinitePackSource
from .finite_pack import FinitePackDefinition
from .local_graph_export import LocalGraphExportUse
from .lifecycle import finish
from .puppy_scenario_configuration import PuppyScenarioPackFiles

class PuppyScenarioFilePolicy:
    """Acquired readable files grant only their original finite-file use."""
    def __init__(self,definition,files,export):
        if type(definition)is not FinitePackDefinition or type(files)is not tuple or len(files)!=3 or any(type(f)is not InvocationFile for f in files)or type(export)is not LocalGraphExportUse:raise ValueError('Actual original file/export owners required')
        self.definition=definition;self.files=files;self.export=export;self.context=export.context
        self.original=(definition,files,export,export.context);self.uid=os.geteuid();self.closed=False
        self.facts=encoded(definition.inputs(*(f.raw for f in files)))
        self.renew()
    def renew(self):
        definition,files,export,context=self.original
        def owner():
            if self.closed or os.geteuid()!=self.uid or self.definition is not definition or self.files is not files or self.export is not export or self.context is not context or export.context is not context:raise PermissionError('Original ordinary file-use owner changed')
        def facts():
            if encoded(definition.inputs(*(f.raw for f in files)))!=self.facts:raise PermissionError('Original complete finite source facts changed')
        finish(None,[owner]+[lambda f=f:f.renew(262144)for f in files]+[export.renew,facts,owner])
    def admit_source(self,facts,context):
        self.renew()
        if context is not self.context or type(facts)not in (dict,MappingProxyType)or encoded(dict(facts))!=self.facts:raise PermissionError('Exact acquired original finite file facts required')

@dataclass(frozen=True)
class PuppyScenarioSourceUse:
    source:FinitePackSource
    pack_bytes:bytes

@contextmanager
def open_puppy_scenario_source(files,*,export,producer,receipt_output):
    if type(files)is not PuppyScenarioPackFiles or type(producer)is not ProducerConfig or type(export)is not LocalGraphExportUse or type(receipt_output)is not type(Path('/')):raise ValueError('Exact original scenario selections/owners required')
    files.__post_init__();producer.__post_init__()
    if export.release.sha256!=files.export.release_sha256 or export.custody_sha256!=files.export.custody_sha256:raise PermissionError('Original selected export pair differs')
    acquired=[];policy=None;dataset=None;source=None;primary=None
    try:
        for path in (files.model,files.graph,files.pack):acquired.append(InvocationFile.read(path,262144))
        definition=FinitePackDefinition(files.name);policy=PuppyScenarioFilePolicy(definition,tuple(acquired),export)
        config=FiniteDatasetConfig(definition,producer.source,producer.bun,producer.git,producer.timeout_seconds,producer.maximum_output_bytes,producer.maximum_receipt_bytes)
        dataset=HeldFiniteDataset(config,files.model,files.graph,receipt_output)
        source=FinitePackSource(definition,files.model,files.graph,dataset,policy=policy,context=export.context)
        source.renew()
        yield PuppyScenarioSourceUse(source,acquired[2].raw)
    except BaseException as error:primary=error
    try:
        finish(primary,([]if source is None else[source.renew])+([]if dataset is None else[dataset.renew])+([policy.renew]if policy is not None else[lambda f=f:f.renew(262144)for f in acquired]))
    finally:
        if policy is not None:policy.closed=True
