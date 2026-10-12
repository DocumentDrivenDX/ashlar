"""Exact finite Fabric HTTP settings; passive selections confer no authority."""
from dataclasses import dataclass
from pathlib import Path
import re


def uuid(value):
    if type(value) is not str or not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', value):
        raise ValueError('Canonical Fabric identifier required')


def capacity(value, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError('Exact finite Fabric capacity required')


@dataclass(frozen=True)
class FabricHttpConfig:
    workspace_id: str
    graph_model_id: str
    account_id: str
    tenant_id: str
    account_user: str
    account_user_type: str
    azure_cli: Path
    azure_config_dir: Path
    azure_cli_version: str
    cli_runtime: object
    http_runtime: object
    maximum_request_bytes: int
    maximum_response_bytes: int
    maximum_total_bytes: int
    maximum_requests: int
    deadline_seconds: int
    maximum_auth_output_bytes: int
    auth_timeout_seconds: int

    def __post_init__(self):
        from .fabric_cli_runtime import (FabricCliRuntimeSelection, FabricHttpRuntimeSelection,
                                         FabricInertCliRuntimeSelection, FabricLoopbackRuntimeSelection)
        if (type(self.cli_runtime) not in (FabricCliRuntimeSelection,FabricInertCliRuntimeSelection)
                or type(self.http_runtime) not in (FabricHttpRuntimeSelection,FabricLoopbackRuntimeSelection)):
            raise ValueError('Exact separately selected runtime profiles required')
        for value in (self.workspace_id, self.graph_model_id, self.account_id, self.tenant_id): uuid(value)
        if (type(self.account_user) is not str or not self.account_user
                or len(self.account_user.encode('utf8')) > 256
                or any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in self.account_user)
                or self.account_user_type != 'user' or type(self.account_user_type) is not str
                or type(self.azure_cli_version) is not str
                or not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', self.azure_cli_version)
                or len(self.azure_cli_version) > 64):
            raise ValueError('Exact selected delegated account/version required')
        for path in (self.azure_cli, self.azure_config_dir):
            if type(path) is not type(Path('/')) or not path.is_absolute():
                raise ValueError('Explicit selected Fabric paths required')
        for name, maximum in (('maximum_request_bytes', 65536), ('maximum_response_bytes', 16777216),
                              ('maximum_total_bytes', 67108864), ('maximum_requests', 128),
                              ('deadline_seconds', 60), ('maximum_auth_output_bytes', 1048576),
                              ('auth_timeout_seconds', 30)):
            capacity(getattr(self, name), maximum)


def fabric_http_config_values(config):
    """Complete exact typed field correspondence; no mutable object equality grant."""
    from dataclasses import fields
    if type(config)is not FabricHttpConfig:raise ValueError('Exact HTTP configuration required')
    config.__post_init__()
    def scalar(value):
        if type(value)is str:return ('str',value)
        if type(value)is int:return ('int',value)
        if type(value)is type(Path('/')):return ('path',str(value))
        if type(value)is tuple:return ('tuple',tuple(scalar(item)for item in value))
        raise ValueError('Exact configuration field carrier required')
    result=[]
    for field in fields(config):
        value=getattr(config,field.name)
        if field.name in ('cli_runtime','http_runtime'):
            result.append((field.name,type(value),tuple((child.name,scalar(getattr(value,child.name)))for child in fields(value))))
        else:result.append((field.name,scalar(value)))
    return tuple(result)
