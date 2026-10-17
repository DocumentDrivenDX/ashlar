"""Finite ordinary delegated Fabric deployment HTTP, CONTRACT-003.

Only explicit creation and observation calls are exposed. No implicit polling,
retry, refresh, journal admission, source grant or loaded-graph policy exists.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import stat
import time
import urllib.error
import urllib.request
from urllib.parse import quote, urlsplit

from .evolution_producer import capture
from .fabric_http_configuration import capacity, uuid
from .lifecycle import finish

_API = 'https://api.fabric.microsoft.com'


@dataclass(frozen=True)
class FabricDeploymentHttpConfig:
    azure_cli: Path
    azure_config_dir: Path
    account_id: str
    tenant_id: str
    account_user: str
    workspace_id: str
    maximum_request_bytes: int = 65536
    maximum_response_bytes: int = 1048576
    maximum_total_bytes: int = 8388608
    maximum_requests: int = 32
    deadline_seconds: int = 60
    auth_timeout_seconds: int = 15
    maximum_auth_output_bytes: int = 65536

    def __post_init__(self):
        for value in (self.account_id, self.tenant_id, self.workspace_id):
            uuid(value)
        for path in (self.azure_cli, self.azure_config_dir):
            if type(path) is not type(Path('/')) or not path.is_absolute() or path.resolve() != path:
                raise ValueError('Canonical absolute CLI/configuration paths required')
        if (type(self.account_user) is not str or not 0 < len(self.account_user) <= 256
                or any(ord(c) < 32 or ord(c) == 127 for c in self.account_user)):
            raise ValueError('Selected delegated user required')
        for name, maximum in (('maximum_request_bytes', 65536), ('maximum_response_bytes', 16777216),
                              ('maximum_total_bytes', 67108864), ('maximum_requests', 128),
                              ('deadline_seconds', 60), ('auth_timeout_seconds', 30),
                              ('maximum_auth_output_bytes', 1048576)):
            capacity(getattr(self, name), maximum)


@dataclass(frozen=True)
class FabricDeploymentResponse:
    status: int
    body: bytes
    headers: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class FabricCreatedGraph:
    """Synchronous 201 native item response; deliberately no invented LRO ID."""
    graph_model_id: str
    response: FabricDeploymentResponse


@dataclass(frozen=True)
class FabricAcceptedCreation:
    """Original 202 response: retain operation_id before manually polling."""
    operation_id: str
    response: FabricDeploymentResponse


def _decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate native JSON member')
            result[key] = value
        return result
    def nonfinite(value):
        raise ValueError('Nonfinite native JSON refused')
    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)
    if type(value) is not dict:
        raise ValueError('Native JSON object required')
    return value


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class FabricDeploymentHttp:
    def __init__(self, *args, **kwargs):
        raise PermissionError('Use open_fabric_deployment_http')

    def __setattr__(self, name, value):
        if name == '_selection' and hasattr(self, name):
            raise AttributeError('Original deployment HTTP selection is immutable')
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if name == '_selection':
            raise AttributeError('Original deployment HTTP selection is immutable')
        object.__delattr__(self, name)

    def _current(self):
        if self._closed:
            raise PermissionError('Original open deployment HTTP interval required')
        selected, origin, deadline, token, opener = self._selection
        if (self._closed or self._config is not self._original
                or tuple(vars(self._config).items()) != selected
                or self._origin != origin or self._deadline != deadline
                or self._token != token or self._opener is not opener):
            raise PermissionError('Original open deployment HTTP interval required')
        if time.monotonic() >= self._deadline:
            raise TimeoutError('Deployment HTTP interval expired')

    def _request(self, method, path, body=b''):
        self._current()
        cfg = self._original
        if type(body) is not bytes or len(body) > cfg.maximum_request_bytes:
            raise ValueError('Bounded original request bytes required')
        if self._requests >= cfg.maximum_requests:
            raise ValueError('Deployment HTTP request capacity exceeded')
        limit = min(cfg.maximum_response_bytes, cfg.maximum_total_bytes - self._bytes)
        if limit <= 0:
            raise ValueError('Deployment HTTP response capacity exceeded')
        self._requests += 1
        request = urllib.request.Request(self._origin + path, data=body if method == 'POST' else None,
            method=method, headers={'Authorization': 'Bearer ' + self._token,
                'Content-Type': 'application/json', 'Accept': 'application/json', 'Accept-Encoding': 'identity'})
        response = None
        primary = None
        result = None
        try:
            try:
                response = self._opener.open(request, timeout=min(15, self._deadline - time.monotonic()))
            except urllib.error.HTTPError as error:
                response = error
            if 300 <= response.code < 400:
                raise PermissionError('Deployment redirect refused')
            if response.headers.get('Content-Encoding') not in (None, 'identity'):
                raise ValueError('Encoded response refused')
            # Bound every response header before exposing selected LRO headers.
            headers = response.headers.items()
            selected = {}
            total = 0
            for key, value in headers:
                total += len(key.encode()) + len(value.encode())
                if total > 16384 or len(value) > 8192 or self._token in value or self._token in key:
                    raise ValueError('Oversized or credential-bearing headers refused')
                key = key.lower()
                if key in ('location', 'x-ms-operation-id', 'retry-after'):
                    if key in selected:
                        raise ValueError('Duplicate operation header')
                    selected[key] = value
            raw = bytearray()
            while True:
                self._current()
                block = response.read1(min(65536, limit - len(raw) + 1))
                if not block:
                    break
                self._bytes += len(block)
                if len(raw) + len(block) > limit:
                    raise ValueError('Deployment response overflow')
                raw.extend(block)
            if self._token.encode() in raw:
                raise PermissionError('Credential-bearing response withheld')
            self._current()
            result = FabricDeploymentResponse(response.code, bytes(raw), tuple(sorted(selected.items())))
        except BaseException as error:
            primary = error
        finish(primary, [response.close] if response is not None else [])
        return result

    def _item(self, raw):
        value = _decode(raw)
        uuid(value.get('id'))
        if value.get('workspaceId') != self._original.workspace_id or value.get('type') != 'GraphModel':
            raise PermissionError('Created item/workspace correspondence refused')
        return value['id']

    def create_graph_model(self, original_payload):
        """Submit once per interval; return before any polling or journal effects."""
        self._current()
        if type(original_payload) is not bytes or not 0 < len(original_payload) <= self._original.maximum_request_bytes:
            raise ValueError('Bounded original creation bytes required')
        value = _decode(original_payload)
        if (set(value) != {'displayName', 'definition'} or type(value['displayName']) is not str
                or not value['displayName'].strip() or len(value['displayName']) > 256
                or type(value['definition']) is not dict):
            raise ValueError('Selected displayName/public definition creation subset required')
        if self._creation_submitted:
            raise PermissionError('Creation already submitted; no implicit retry')
        self._creation_submitted = True  # uncertain failures cannot reopen submission
        response = self._request('POST', '/v1/workspaces/' + self._original.workspace_id + '/graphModels', original_payload)
        if response.status == 201:
            return FabricCreatedGraph(self._item(response.body), response)
        if response.status != 202 or response.body:
            raise ValueError('Creation did not return selected 201/empty 202')
        headers = dict(response.headers)
        operation = headers.get('x-ms-operation-id')
        uuid(operation)
        self._operation_headers(headers, operation, required=True)
        return FabricAcceptedCreation(operation, response)

    def _operation_headers(self, headers, operation, *, required=False):
        if 'x-ms-operation-id' in headers and headers['x-ms-operation-id'] != operation:
            raise PermissionError('Original operation ID differs')
        if required and not {'location', 'retry-after'} <= headers.keys():
            raise ValueError('Original LRO location/delay required')
        if 'retry-after' in headers and re.fullmatch('[0-9]{1,5}', headers['retry-after']) is None:
            raise ValueError('Bounded Retry-After required')
        if 'location' in headers:
            location = urlsplit(headers['location'])
            allowed_host = location.hostname == 'api.fabric.microsoft.com' or re.fullmatch(
                r'wabi-[a-z0-9-]+-redirect\.analysis\.windows\.net', location.hostname or '')
            if (location.scheme != 'https' or not allowed_host or location.port not in (None, 443)
                    or location.username or location.password or location.query or location.fragment
                    or location.path not in ('/v1/operations/' + operation, '/v1/operations/' + operation + '/result')):
                raise PermissionError('Unselected original operation location')
        # Location is retained only. Every request uses the canonical API origin.

    def creation_state(self, operation_id):
        uuid(operation_id)
        response = self._request('GET', '/v1/operations/' + operation_id)
        if response.status != 200:
            raise ValueError('Creation operation state refused')
        self._operation_headers(dict(response.headers), operation_id)
        if _decode(response.body).get('status') not in ('NotStarted', 'Running', 'Succeeded', 'Failed'):
            raise ValueError('Unqualified creation status')
        return response

    def operation_result(self, operation_id):
        """Explicit original LRO result; caller owns its operation-specific meaning."""
        uuid(operation_id)
        response = self._request('GET', '/v1/operations/' + operation_id + '/result')
        if response.status != 200:
            raise ValueError('Original operation result refused')
        self._operation_headers(dict(response.headers), operation_id)
        _decode(response.body)
        return response

    def creation_result(self, operation_id):
        response = self.operation_result(operation_id)
        self._item(response.body)
        return response

    def graph_definition(self, graph_model_id):
        """Return original 200 definition or 202 headers without implicit polling."""
        uuid(graph_model_id)
        response = self._request('POST', '/v1/workspaces/' + self._original.workspace_id
                                 + '/graphModels/' + graph_model_id + '/getDefinition', b'')
        if response.status == 200:
            value = _decode(response.body)
            if type(value.get('definition')) is not dict:
                raise ValueError('Original public graph definition required')
        elif response.status == 202 and not response.body:
            headers = dict(response.headers)
            operation = headers.get('x-ms-operation-id')
            uuid(operation)
            self._operation_headers(headers, operation, required=True)
        else:
            raise ValueError('Original graph definition request refused')
        return response

    def item_jobs(self, graph_model_id, continuation_token=None):
        uuid(graph_model_id)
        path = '/v1/workspaces/' + self._original.workspace_id + '/items/' + graph_model_id + '/jobs/instances'
        if continuation_token is not None:
            if type(continuation_token) is not str or not 0 < len(continuation_token) <= 8192:
                raise ValueError('Bounded opaque continuation required')
            path += '?continuationToken=' + quote(continuation_token, safe='')
        response = self._request('GET', path)
        if response.status != 200:
            raise ValueError('Item job listing refused')
        value = _decode(response.body)
        if type(value.get('value')) is not list:
            raise ValueError('Item job list required')
        for job in value['value']:
            self._job(job, graph_model_id)
        return response

    def _job(self, value, item):
        if type(value) is not dict or value.get('itemId') != item:
            raise PermissionError('Exact original item job required')
        uuid(value.get('id'))
        for key in ('status', 'jobType', 'invokeType'):
            if type(value.get(key)) is not str or not value[key]:
                raise ValueError('Native job envelope required')

    def item_job(self, graph_model_id, job_id):
        uuid(graph_model_id)
        uuid(job_id)
        response = self._request('GET', '/v1/workspaces/' + self._original.workspace_id + '/items/' + graph_model_id + '/jobs/instances/' + job_id)
        if response.status != 200:
            raise ValueError('Item job observation refused')
        value = _decode(response.body)
        self._job(value, graph_model_id)
        if value['id'] != job_id:
            raise PermissionError('Exact retained job ID differs')
        self._operation_headers({k: v for k, v in response.headers if k == 'retry-after'}, job_id)
        return response


@contextmanager
def open_fabric_deployment_http(config):
    """Ordinary finite delegated workspace transport; no publication grant."""
    with _open(config, _API) as owner:
        yield owner


@contextmanager
def open_fabric_deployment_loopback_for_test(config, origin):
    """Explicit real HTTP test surface; never a cloud/deployment qualification."""
    if type(origin) is not str or re.fullmatch(r'http://127\.0\.0\.1:[0-9]{1,5}', origin) is None:
        raise ValueError('Explicit IPv4 loopback test origin required')
    port = urlsplit(origin).port
    if port is None or not 1 <= port <= 65535:
        raise ValueError('Finite loopback port required')
    with _open(config, origin) as owner:
        yield owner


@contextmanager
def _open(config, origin):
    if type(config) is not FabricDeploymentHttpConfig:
        raise ValueError('Exact deployment HTTP configuration required')
    config.__post_init__()
    if not config.azure_cli.is_file() or not os.access(config.azure_cli, os.X_OK):
        raise ValueError('Selected executable required')
    directory = config.azure_config_dir.stat()
    if not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.getuid() or directory.st_mode & 0o022:
        raise PermissionError('Owned non-writable-by-others CLI directory required')
    deadline = time.monotonic() + config.deadline_seconds
    environment = {'PATH': '/usr/bin:/bin', 'AZURE_CONFIG_DIR': str(config.azure_config_dir), 'HOME': str(Path.home())}
    def command(args):
        remaining = deadline - time.monotonic()
        if remaining < 1:
            raise TimeoutError('Finite authentication deadline exceeded')
        out, err = capture([str(config.azure_cli), *args, '--output', 'json', '--only-show-errors'],
            cwd=config.azure_config_dir, environment=environment,
            timeout_seconds=min(config.auth_timeout_seconds, int(remaining)), maximum_output_bytes=config.maximum_auth_output_bytes)
        if err:
            raise PermissionError('Selected ordinary authentication refused')
        return _decode(out)
    def account():
        value = command(['account', 'show', '--subscription', config.account_id])
        if (value.get('id'), value.get('tenantId'), value.get('user', {}).get('name'),
                value.get('user', {}).get('type'), value.get('state'), value.get('environmentName')) != (
                config.account_id, config.tenant_id, config.account_user, 'user', 'Enabled', 'AzureCloud'):
            raise PermissionError('Selected delegated account differs')
    owner = None
    primary = None
    try:
        account()
        token = command(['account', 'get-access-token', '--subscription', config.account_id, '--resource', _API])
        secret = token.get('accessToken')
        expiry = token.get('expires_on')
        if (token.get('tenant') != config.tenant_id or token.get('subscription') != config.account_id
                or token.get('tokenType') != 'Bearer' or type(secret) is not str or not 0 < len(secret) <= 32768
                or any(ord(c) <= 32 or ord(c) >= 127 for c in secret)
                or type(expiry) not in (str, int) or not str(expiry).isascii() or not str(expiry).isdecimal()
                or int(expiry) <= time.time() + config.deadline_seconds):
            raise PermissionError('Selected finite delegated token refused')
        owner = object.__new__(FabricDeploymentHttp)
        owner._config = owner._original = config
        owner._origin = origin
        owner._deadline = deadline
        owner._token = secret
        owner._closed = False
        owner._requests = owner._bytes = 0
        owner._creation_submitted = False
        owner._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        owner._selection = (tuple(vars(config).items()), origin, deadline, secret, owner._opener)
        yield owner
    except BaseException as error:
        primary = error
    def close():
        if owner is not None:
            owner._closed = True
            owner._token = None
            object.__setattr__(owner, '_selection', None)
    finish(primary, [account, close])
