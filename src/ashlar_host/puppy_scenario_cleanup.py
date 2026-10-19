# SPDX-License-Identifier: MIT OR Apache-2.0
"""Parent cleanup for an actual returned original17 container ID only."""
from dataclasses import fields
import hashlib
import json
from pathlib import Path
import re
import time
from .evolution_producer import capture
from .lifecycle import finish
from .puppy_finite_local import PuppyScenarioContainerReceipt
from .puppy_scenario_configuration import PuppyScenarioInvocationConfig


def decode_scenario_creation(raw, config, nonce):
    """Private child channel describes creation; Docker inspection admits cleanup."""
    if type(config) is not PuppyScenarioInvocationConfig:
        raise ValueError('Exact parent scenario configuration required')
    config.__post_init__()
    if type(nonce) is not str or re.fullmatch('[0-9a-f]{32}', nonce) is None:
        raise ValueError('Exact invocation nonce required')
    if type(raw) is not bytes or not 0 < len(raw) <= 4096 or not raw.endswith(b'\n') or raw.count(b'\n') != 1:
        raise ValueError('One complete bounded creation event required')
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value: raise ValueError('Duplicate creation member')
            value[key] = item
        return value
    value = json.loads(raw, object_pairs_hook=pairs)
    if type(value) is not dict or set(value) != {'type', 'receipt'} or value['type'] != 'created':
        raise ValueError('Closed creation event required')
    selected = value['receipt']
    if type(selected) is not dict or set(selected) != {f.name for f in fields(PuppyScenarioContainerReceipt)}:
        raise ValueError('Closed actual container description required')
    runtime = config.runtime
    expected = {'image': runtime.image, 'name': runtime.name, 'invocation_nonce': nonce,
        'readonly_mount_source': str(config.output/'prepared/scenarios.duckdb'),
        'readonly_mount_target': '/tmp/scenarios.duckdb', 'http_port': runtime.http_port,
        'bolt_port': runtime.bolt_port, 'gremlin_port': runtime.gremlin_port,
        'cpus': 2, 'memory_bytes': 3*1024**3}
    for key, item in expected.items():
        if type(selected[key]) is not type(item) or selected[key] != item:
            raise PermissionError('Original container description differs')
    if type(selected['container_id']) is not str or re.fullmatch('[0-9a-f]{64}', selected['container_id']) is None:
        raise ValueError('Actual returned full container ID required')
    return PuppyScenarioContainerReceipt(**{**selected,
        'readonly_mount_source': Path(selected['readonly_mount_source'])})


def cleanup_scenario_container(config, receipt, *, invocation_nonce, deadline=None):
    """Inspect nonce/image/resources/mount/ports before removing this exact ID.

    Absence is checked through a successful ID-filtered listing. A Docker error
    or a foreign description never substitutes for absence or authorizes removal.
    """
    if type(config) is not PuppyScenarioInvocationConfig or type(receipt) is not PuppyScenarioContainerReceipt:
        raise ValueError('Exact parent selections and creation description required')
    # Re-admit the typed description against the immutable parent selections.
    wire = {f.name: getattr(receipt, f.name) for f in fields(receipt)}
    wire['readonly_mount_source'] = str(receipt.readonly_mount_source)
    decode_scenario_creation((json.dumps({'type': 'created', 'receipt': wire})+'\n').encode(), config, invocation_nonce)
    runtime = config.runtime
    def docker(arguments):
        timeout = 10 if deadline is None else min(10, int(deadline-time.monotonic()))
        if timeout < 1: raise TimeoutError('Owned container cleanup deadline elapsed')
        return capture([str(runtime.docker), *arguments], cwd=config.output.parent,
            environment={'PATH': '/usr/bin:/bin', 'DOCKER_HOST': runtime.docker_host},
            timeout_seconds=timeout, maximum_output_bytes=1048576)[0]
    def absent():
        raw = docker(['container', 'ls', '--all', '--no-trunc', '--filter',
                      'id='+receipt.container_id, '--format', '{{.ID}}'])
        if raw not in (b'', (receipt.container_id+'\n').encode()):
            raise PermissionError('Exact owned container listing required')
        return not raw
    if absent():
        return {'container_id': receipt.container_id, 'absent': True, 'cleanup_complete': True}
    raw = docker(['inspect', receipt.container_id])
    value = json.loads(raw)
    if type(value) is not list or len(value) != 1: raise PermissionError('Exact owned inspection required')
    row = value[0]
    host = row['HostConfig']
    if (row['Id'] != receipt.container_id or row['Image'] != receipt.image
            or row['Name'] != '/'+receipt.name
            or row['Config'].get('Labels', {}).get('ashlar.scenario.invocation') != receipt.invocation_nonce
            or type(host['NanoCpus']) is not int or host['NanoCpus'] != 2000000000
            or type(host['Memory']) is not int or host['Memory'] != receipt.memory_bytes):
        raise PermissionError('Current container owner selections differ')
    mounts = [m for m in row['Mounts'] if m['Destination'] == receipt.readonly_mount_target]
    if len(mounts) != 1 or mounts[0]['Source'] != str(receipt.readonly_mount_source) or mounts[0]['RW'] is not False or mounts[0]['Type'] != 'bind':
        raise PermissionError('Current original readonly mount differs')
    ports = {'8081/tcp': receipt.http_port, '7687/tcp': receipt.bolt_port}
    if receipt.gremlin_port is not None: ports['8182/tcp'] = receipt.gremlin_port
    if set(host['PortBindings']) != set(ports):
        raise PermissionError('Closed selected loopback bindings required')
    for key, port in ports.items():
        expected = [{'HostIp': '127.0.0.1', 'HostPort': str(port)}]
        if host['PortBindings'].get(key) != expected:
            raise PermissionError('Current selected loopback binding differs')
        if row['State']['Running'] and row['NetworkSettings']['Ports'].get(key) != expected:
            raise PermissionError('Current running loopback endpoint differs')
    # Independent attempts, including cleanup after an unknown submission outcome.
    finish(None, [lambda: docker(['stop', '--time', '5', receipt.container_id]),
                  lambda: docker(['rm', '--force', receipt.container_id])])
    if not absent(): raise RuntimeError('Owned container remains after cleanup')
    return {'container_id': receipt.container_id, 'absent': True,
            'inspection_sha256': hashlib.sha256(raw).hexdigest(), 'cleanup_complete': True}
