# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fresh-only OneLake release-byte upload under an actual local mutation lease.

Byte correspondence is not publication/source/native/load authority. The caller
must retain those independent owners; this module never creates/refreshes a graph.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import urllib.error
import urllib.request

from .evolution_producer import capture
from .fabric_deployment_http import FabricDeploymentHttpConfig
from .fabric_http_configuration import capacity, uuid
from .fabric_release_journal import FabricMutation
from .lifecycle import finish
from urllib.parse import urlsplit

_DFS = 'https://onelake.dfs.fabric.microsoft.com'
_COMMIT = '_delta_log/00000000000000000000.json'


@dataclass(frozen=True)
class OneLakeUploadConfig:
    authentication: FabricDeploymentHttpConfig
    lakehouse_id: str
    maximum_file_bytes: int = 1048576
    maximum_table_bytes: int = 8388608

    def __post_init__(self):
        if type(self.authentication) is not FabricDeploymentHttpConfig:
            raise ValueError('Exact finite delegated account selection required')
        self.authentication.__post_init__()
        uuid(self.lakehouse_id)
        capacity(self.maximum_file_bytes, 1048576)
        capacity(self.maximum_table_bytes, 8388608)


@dataclass(frozen=True)
class OneLakeReleaseFile:
    path: str
    payload: bytes

    def __post_init__(self):
        if (type(self.path) is not str or (self.path != _COMMIT
                and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,240}\.parquet', self.path) is None)
                or type(self.payload) is not bytes or not self.payload):
            raise ValueError('Selected nonempty flat data or version-zero commit bytes required')


@dataclass(frozen=True)
class OneLakeReleaseTable:
    path: str
    files: tuple[OneLakeReleaseFile, ...]

    def __post_init__(self):
        if type(self.path) is not str or re.fullmatch(r'Tables/[A-Za-z0-9_]{1,64}/[A-Za-z0-9_]{1,128}', self.path) is None:
            raise ValueError('Explicit schema/release table path required')
        if any(part.lower() in ('latest', 'current') for part in self.path.split('/')):
            raise ValueError('Mutable source alias refused')
        if type(self.files) is not tuple or not 2 <= len(self.files) <= 64 or any(type(x) is not OneLakeReleaseFile for x in self.files):
            raise ValueError('Immutable bounded data/commit selection required')
        names = [x.path for x in self.files]
        if len(set(names)) != len(names) or names.count(_COMMIT) != 1:
            raise ValueError('One original version-zero commit and distinct data files required')
        for item in self.files:
            item.__post_init__()

    def inventory(self):
        return tuple((f.path, len(f.payload), hashlib.sha256(f.payload).hexdigest()) for f in sorted(self.files, key=lambda x: x.path))

    def sha256(self):
        """SHA256 of canonical JSON ordered [relative path, byte count, SHA256] rows."""
        return hashlib.sha256(json.dumps(self.inventory(), separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()


@dataclass(frozen=True)
class OneLakeUploadReceipt:
    location: str
    inventory_sha256: str
    inventory: tuple
    responses: tuple


class OneLakeOperationRefused(ValueError):
    """Original bounded credential-checked refusal, retained after owner closure.

    The message deliberately omits native body/header content. Callers may retain
    response in private diagnostic evidence; it grants no retry or write authority.
    """
    def __init__(self, response):
        if (type(response) is not tuple or len(response) != 5
                or type(response[0]) is not str or type(response[1]) is not str
                or type(response[2]) is not int or not 100 <= response[2] <= 599
                or type(response[3]) is not bytes or type(response[4]) is not tuple
                or any(type(pair) is not tuple or len(pair) != 2
                       or any(type(value) is not str for value in pair) for pair in response[4])):
            raise ValueError('Exact immutable OneLake refusal response required')
        self._response = response
        super().__init__('OneLake operation refused (HTTP ' + str(response[2]) + '); no retry or overwrite')

    def __setattr__(self, name, value):
        if name == '_response' and hasattr(self, name):
            raise AttributeError('Original OneLake refusal response is immutable')
        super().__setattr__(name, value)

    def __delattr__(self, name):
        if name == '_response':
            raise AttributeError('Original OneLake refusal response is immutable')
        super().__delattr__(name)

    @property
    def response(self):
        return self._response


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class OneLakeReleaseUpload:
    def __init__(self, *args, **kwargs):
        raise PermissionError('Use open_onelake_release_upload')

    def __setattr__(self, name, value):
        if name == '_selected' and hasattr(self, name):
            raise AttributeError('Original OneLake selection is immutable')
        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if name == '_selected':
            raise AttributeError('Original OneLake selection is immutable')
        object.__delattr__(self, name)

    def _current(self):
        if self._closed:
            raise PermissionError('Original open OneLake upload required')
        cfg, facts, mutation, candidate, origin, token, deadline, opener = self._selected
        current_facts = (tuple(vars(self._config.authentication).items()), self._config.lakehouse_id,
                         self._config.maximum_file_bytes, self._config.maximum_table_bytes)
        if (self._config is not cfg or current_facts != facts or self._mutation is not mutation
                or self._candidate != candidate or self._origin != origin or self._token != token
                or self._deadline != deadline or self._opener is not opener):
            raise PermissionError('Original OneLake transport selection changed')
        self._mutation.renew()
        if time.monotonic() >= deadline:
            raise TimeoutError('OneLake upload deadline exceeded')

    def _request(self, method, path, query='', payload=b'', *, fresh=False, expected=200):
        self._current()
        cfg = self._config.authentication
        if type(payload) is not bytes or len(payload) > cfg.maximum_request_bytes:
            raise ValueError('Selected OneLake request byte capacity exceeded')
        limit = min(cfg.maximum_response_bytes, cfg.maximum_total_bytes - self._bytes - len(payload))
        if self._requests >= cfg.maximum_requests or limit < 0:
            raise ValueError('OneLake request/byte capacity exceeded')
        self._requests += 1
        self._bytes += len(payload)
        headers = {'Authorization': 'Bearer ' + self._token, 'x-ms-version': '2021-06-08', 'Accept-Encoding': 'identity'}
        if fresh:
            headers['If-None-Match'] = '*'
        request = urllib.request.Request(self._origin + '/' + cfg.workspace_id + '/' + self._config.lakehouse_id + '/' + path + query,
            data=payload if method != 'GET' else None, method=method, headers=headers)
        response = None
        primary = None
        raw = bytearray()
        retained_headers = ()
        code = None
        try:
            try:
                response = self._opener.open(request, timeout=min(15, self._deadline - time.monotonic()))
            except urllib.error.HTTPError as error:
                response = error
            code = response.code
            selected = []
            total = 0
            for key, value in response.headers.items():
                total += len(key.encode()) + len(value.encode())
                if total > 16384 or self._token in key or self._token in value:
                    raise ValueError('OneLake response headers refused')
                if key.lower() in ('etag', 'x-ms-request-id', 'content-length'):
                    selected.append((key.lower(), value))
            retained_headers = tuple(selected)
            if 300 <= code < 400 or response.headers.get('Content-Encoding') not in (None, 'identity'):
                raise PermissionError('OneLake redirect/encoding refused')
            while True:
                self._current()
                block = response.read1(min(65536, limit - len(raw) + 1))
                if not block:
                    break
                self._bytes += len(block)
                if len(raw) + len(block) > limit:
                    raise ValueError('OneLake response capacity exceeded')
                raw.extend(block)
            if self._token.encode() in raw:
                raise PermissionError('Credential-bearing OneLake response withheld')
            self._current()
            self._observations.append((method, path + query, code, bytes(raw), retained_headers))
            if code != expected:
                raise OneLakeOperationRefused(self._observations[-1])
        except BaseException as error:
            primary = error
        finish(primary, ([response.close] if response is not None else []) + [self._mutation.renew])
        receipt = (method, path + query, code, bytes(raw), retained_headers)
        return bytes(raw), receipt

    def observations(self):
        """Complete credential-checked original responses, including status refusals."""
        return tuple(self._observations)

    def _table(self, table):
        self._current()
        if type(table) is not OneLakeReleaseTable:
            raise ValueError('Exact original table bytes required')
        table.__post_init__()
        if any(len(f.payload) > min(self._config.maximum_file_bytes, self._config.authentication.maximum_request_bytes) for f in table.files) or sum(len(f.payload) for f in table.files) > self._config.maximum_table_bytes:
            raise ValueError('Selected OneLake table/file capacity exceeded')
        cfg = self._config.authentication
        location = 'abfss://' + cfg.workspace_id + '@onelake.dfs.fabric.microsoft.com/' + self._config.lakehouse_id + '/' + table.path
        intent = self._mutation.candidate(self._candidate)[0]
        if intent.workspace_id != cfg.workspace_id or (location, table.sha256()) not in intent.sources:
            raise PermissionError('Table bytes/location differ from original candidate intent')
        return location

    def upload(self, table):
        """Fresh-only upload, full readback, data before commit. Partial files remain."""
        location = self._table(table)
        if self._mutation.candidate(self._candidate)[1] != 'recorded':
            raise PermissionError('Staging requires original unsubmitted candidate')
        if table.path in self._attempted:
            raise PermissionError('Table upload already attempted; explicit readback only')
        self._attempted.add(table.path)
        receipts = []
        for directory in (table.path, table.path + '/_delta_log'):
            receipts.append(self._request('PUT', directory, '?resource=directory', fresh=True, expected=201)[1])
        ordered = sorted(table.files, key=lambda f: (f.path == _COMMIT, f.path))
        for item in ordered:
            path = table.path + '/' + item.path
            receipts.append(self._request('PUT', path, '?resource=file', fresh=True, expected=201)[1])
            receipts.append(self._request('PATCH', path, '?action=append&position=0', item.payload, expected=202)[1])
            receipts.append(self._request('PATCH', path, '?action=flush&position=' + str(len(item.payload)))[1])
            raw, receipt = self._request('GET', path)
            receipts.append(receipt)
            if raw != item.payload:
                raise PermissionError('Original OneLake byte readback differs')
        self._current()
        return OneLakeUploadReceipt(location, table.sha256(), table.inventory(), tuple(receipts))

    def readback(self, table):
        """Explicit recovery observation only; never writes or repairs partial output."""
        location = self._table(table)
        receipts = []
        for item in table.files:
            raw, receipt = self._request('GET', table.path + '/' + item.path)
            receipts.append(receipt)
            if raw != item.payload:
                raise PermissionError('Original OneLake byte readback differs')
        self._current()
        return OneLakeUploadReceipt(location, table.sha256(), table.inventory(), tuple(receipts))


@contextmanager
def open_onelake_release_upload(config, mutation, candidate_id):
    with _open(config, mutation, candidate_id, _DFS) as owner:
        yield owner


@contextmanager
def open_onelake_upload_loopback_for_test(config, mutation, candidate_id, origin):
    if type(origin) is not str or re.fullmatch(r'http://127\.0\.0\.1:[0-9]{1,5}', origin) is None:
        raise ValueError('Explicit IPv4 loopback test origin required')
    port = urlsplit(origin).port
    if port is None or not 1 <= port <= 65535:
        raise ValueError('Finite loopback port required')
    with _open(config, mutation, candidate_id, origin) as owner:
        yield owner


@contextmanager
def _open(config, mutation, candidate_id, origin):
    if type(config) is not OneLakeUploadConfig or type(mutation) is not FabricMutation:
        raise ValueError('Exact OneLake configuration and live mutation permit required')
    config.__post_init__()
    mutation.renew()
    uuid(candidate_id)
    if mutation.candidate(candidate_id)[0].workspace_id != config.authentication.workspace_id:
        raise PermissionError('Original candidate workspace differs')
    cfg = config.authentication
    if not cfg.azure_cli.is_file() or not os.access(cfg.azure_cli, os.X_OK):
        raise ValueError('Selected executable required')
    info = cfg.azure_config_dir.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise PermissionError('Selected CLI directory must be owned and not writable by others')
    deadline = time.monotonic() + cfg.deadline_seconds
    environment = {'PATH': '/usr/bin:/bin', 'AZURE_CONFIG_DIR': str(cfg.azure_config_dir), 'HOME': str(Path.home())}
    def command(args):
        remaining = deadline - time.monotonic()
        if remaining < 1:
            raise TimeoutError('Finite storage authentication deadline exceeded')
        out, err = capture([str(cfg.azure_cli), *args, '--output', 'json', '--only-show-errors'],
            cwd=cfg.azure_config_dir, environment=environment, timeout_seconds=min(cfg.auth_timeout_seconds, int(remaining)),
            maximum_output_bytes=cfg.maximum_auth_output_bytes)
        if err:
            raise PermissionError('Storage authentication refused')
        return json.loads(out)
    def account():
        value = command(['account', 'show', '--subscription', cfg.account_id])
        if (value.get('id'), value.get('tenantId'), value.get('user', {}).get('name'), value.get('user', {}).get('type'),
                value.get('state'), value.get('environmentName')) != (cfg.account_id, cfg.tenant_id, cfg.account_user, 'user', 'Enabled', 'AzureCloud'):
            raise PermissionError('Selected storage account differs')
    owner = None
    primary = None
    try:
        account()
        token = command(['account', 'get-access-token', '--subscription', cfg.account_id, '--resource', 'https://storage.azure.com/'])
        secret = token.get('accessToken')
        expiry = token.get('expires_on')
        if (token.get('tenant') != cfg.tenant_id or token.get('subscription') != cfg.account_id or token.get('tokenType') != 'Bearer'
                or type(secret) is not str or not 0 < len(secret) <= 32768 or any(ord(c) <= 32 or ord(c) >= 127 for c in secret)
                or type(expiry) not in (str, int) or not str(expiry).isascii() or not str(expiry).isdecimal()
                or int(expiry) <= time.time() + cfg.deadline_seconds):
            raise PermissionError('Selected finite storage token refused')
        owner = object.__new__(OneLakeReleaseUpload)
        owner._config = config
        owner._mutation = mutation
        owner._candidate = candidate_id
        owner._origin = origin
        owner._token = secret
        owner._deadline = deadline
        owner._closed = False
        owner._attempted = set()
        owner._observations = []
        owner._requests = owner._bytes = 0
        owner._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
        facts = (tuple(vars(cfg).items()), config.lakehouse_id, config.maximum_file_bytes, config.maximum_table_bytes)
        owner._selected = (config, facts, mutation, candidate_id, origin, secret, deadline, owner._opener)
        yield owner
    except BaseException as error:
        primary = error
    def close():
        if owner is not None:
            owner._closed = True
            owner._token = None
            object.__setattr__(owner, '_selected', None)
    def current():
        if owner is not None:
            owner._current()
    finish(primary, [account, current, mutation.renew, close])
