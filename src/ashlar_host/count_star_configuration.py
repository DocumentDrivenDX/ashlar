"""Explicit bounded 041 commerce host configuration; historical defaults unchanged."""
from dataclasses import dataclass
from pathlib import Path
from .config import HostError,ProducerConfig,PrivatePostgresConfig
from .path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig

@dataclass(frozen=True)
class QueryCommerceCountStarConfig:
    index:Path
    installation:Path
    publication:Path
    output:Path
    jars:Path
    model:Path
    graph:Path
    producer:ProducerConfig
    postgres:PrivatePostgresConfig
    maximum_artifact_bytes:int
    capture:PathCaptureConfig
    decoder:PathDecodeConfig
    def __post_init__(self):
        for path in (self.index,self.installation,self.publication,self.output,self.jars,self.model,self.graph):
            if not isinstance(path,Path)or not path.is_absolute():raise HostError('absolute-path-required')
        if type(self.producer)is not ProducerConfig or type(self.postgres)is not PrivatePostgresConfig or type(self.capture)is not PathCaptureConfig or type(self.decoder)is not PathDecodeConfig:raise HostError('invalid-configuration')
        for value,limit in ((self.maximum_artifact_bytes,16*1024*1024),(self.capture.maximum_rows,1000),(self.capture.maximum_cell_bytes,16*1024*1024),(self.capture.maximum_total_cell_bytes,64*1024*1024)):
            if type(value)is not int or not 1<=value<=limit:raise HostError('finite-bound-required')
        if self.decoder.maximum_cell_bytes>self.capture.maximum_cell_bytes:raise HostError('decoder-exceeds-capture-bound')
