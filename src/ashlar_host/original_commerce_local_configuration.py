"""Explicit local original-commerce selections; no installation or discovery."""
from dataclasses import dataclass
from pathlib import Path
import re
import hashlib
import os
import sys
from .evolution_producer import snapshot
from .evolution_cli import InvocationFile
from .ack import AckScope
from .config import ProducerConfig
from .delta_custody import operation_capacity
from .original_commerce_publication import OriginalCommercePublicationConfig


def _path(value):
    if type(value) is not type(Path()) or not value.is_absolute():
        raise ValueError('Exact absolute local owner path required')


@dataclass(frozen=True)
class OriginalCommerceLocalInvocationCustody:
    files: tuple

    def __post_init__(self):
        if type(self.files)is not tuple or not 1<=len(self.files)<=2 or any(type(f)is not InvocationFile for f in self.files):
            raise ValueError('Exact original invocation files required')
        for file in self.files:file.__post_init__()

    def renew(self):
        self.__post_init__()
        for file in self.files:file.renew(262144)


def _custody(value):
    if value is not None:
        if type(value)is not OriginalCommerceLocalInvocationCustody:raise ValueError('Exact typed configuration custody required')
        value.__post_init__()


@dataclass(frozen=True)
class OriginalCommerceLocalRuntime:
    python: Path
    python_sha256: str
    java_home: Path
    java_sha256: str
    jvm_config_sha256: str

    def __post_init__(self):
        _path(self.python);_path(self.java_home)
        if any(type(v)is not str or re.fullmatch('[0-9a-f]{64}',v)is None for v in
               (self.python_sha256,self.java_sha256,self.jvm_config_sha256)):
            raise ValueError('Explicit selected runtime bytes required')

    def renew(self):
        self.__post_init__()
        if Path(sys.executable)!=self.python or os.environ.get('JAVA_HOME')!=str(self.java_home):
            raise PermissionError('Current explicitly selected interpreter/JAVA_HOME required')
        for path,digest,bound in ((self.python,self.python_sha256,67108864),
                (self.java_home/'bin/java',self.java_sha256,1048576),
                (self.java_home/'lib/jvm.cfg',self.jvm_config_sha256,65536)):
            if hashlib.sha256(snapshot(path,bound)).hexdigest()!=digest:
                raise PermissionError('Selected original runtime bytes changed')


@dataclass(frozen=True)
class OriginalCommerceLocalInitializationConfig:
    root: Path
    output: Path
    installation_id: str
    model: Path
    graph: Path
    jars: Path
    producer: ProducerConfig
    capacity: object
    runtime: OriginalCommerceLocalRuntime
    custody: object = None

    def __post_init__(self):
        for value in (self.root,self.output,self.model,self.graph,self.jars):
            _path(value)
        if (type(self.installation_id) is not str or
                re.fullmatch('[A-Za-z0-9][A-Za-z0-9:_-]{0,1023}',self.installation_id) is None or
                type(self.producer) is not ProducerConfig):
            raise ValueError('Explicit original local installation required')
        self.producer.__post_init__()
        if type(self.runtime)is not OriginalCommerceLocalRuntime:raise ValueError('Exact explicit runtime selection required')
        self.runtime.__post_init__()
        _custody(self.custody)
        if self.root.parent.resolve()!=self.root.parent or self.output.parent.resolve()!=self.output.parent:
            raise ValueError('Canonical original installation/observation parents required')
        if self.root==self.output or self.root in self.output.parents or self.output in self.root.parents:
            raise ValueError('Native installation and observations must be separate')
        object.__setattr__(self,'capacity',operation_capacity(self.capacity))

    def publication_inputs(self):
        self.__post_init__()
        return OriginalCommercePublicationConfig(self.output,self.model,self.graph,self.jars,self.producer)


@dataclass(frozen=True)
class OriginalCommerceLocalSource:
    host: str
    port: int
    database: str
    authentication: str
    scope: AckScope
    source_schema: str
    source_signature_sha256: str
    writer_role: str
    reader_role: str
    ack_role: str

    def __post_init__(self):
        if (type(self.host)is not str or self.host!='127.0.0.1'or type(self.port)is not int or not 1024<=self.port<=65535 or
            type(self.database)is not str or self.database!='truss_e2e'or
            type(self.authentication)is not str or self.authentication!='isolated-loopback-trust'or type(self.scope)is not AckScope):
            raise ValueError('Explicit isolated loopback fixture only')
        values={name:getattr(self.scope,name)for name in ('service_schema','scope_id','installation_id','consumer','feed','epoch')}
        if any(type(v)is not str for v in values.values()):raise ValueError('Passive exact original scope required')
        object.__setattr__(self,'scope',AckScope(**values))
        if (self.scope.feed,self.scope.epoch)!=('private-original-commerce-fixture','original-commerce-v1'):
            raise ValueError('Exact original commerce source scope required')
        if (any(type(v)is not str or re.fullmatch('[a-z][a-z0-9_]{0,62}',v)is None for v in
                (self.source_schema,self.writer_role,self.reader_role,self.ack_role))or
            len({self.writer_role,self.reader_role,self.ack_role})!=3 or
            type(self.source_signature_sha256)is not str or re.fullmatch('[0-9a-f]{64}',self.source_signature_sha256)is None):
            raise ValueError('Distinct ordinary roles and original schema/signature required')


@dataclass(frozen=True)
class OriginalCommerceLocalPublicationConfig:
    installation: Path
    installation_sha256: str
    publication: OriginalCommercePublicationConfig
    source: OriginalCommerceLocalSource
    runtime: OriginalCommerceLocalRuntime
    custody: object = None

    def __post_init__(self):
        _path(self.installation)
        if (type(self.installation_sha256)is not str or
            re.fullmatch('[0-9a-f]{64}',self.installation_sha256)is None or
            type(self.publication)is not OriginalCommercePublicationConfig):
            raise ValueError('Explicit existing installation correspondence required')
        self.publication.__post_init__()
        if type(self.source)is not OriginalCommerceLocalSource:raise ValueError('Exact isolated ordinary source selection required')
        self.source.__post_init__()
        if type(self.runtime)is not OriginalCommerceLocalRuntime:raise ValueError('Exact explicit runtime selection required')
        self.runtime.__post_init__()
        _custody(self.custody)
