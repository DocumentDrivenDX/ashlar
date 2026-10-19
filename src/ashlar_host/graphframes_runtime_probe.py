# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded-host caller's genuine selected-worker admission, without a Spark session."""
import hashlib,json,sys
from pathlib import Path
from .graphframes_release import GraphFramesRuntimeConfig,VERSIONS
from .graphframes_local import GraphFramesLocalRuntimeSelection,LocalGraphFramesRuntimePolicy
from .evolution_producer import snapshot


def runtime_bytes(runtime):
    names=('python','python_sha256','java_home','java_sha256','worker_revision',
        'worker_inventory_sha256','sdk_inventory_sha256','jars','work','timeout_seconds',
        'maximum_stream_bytes','maximum_evidence_bytes')
    return json.dumps({name:str(getattr(runtime,name))if type(getattr(runtime,name))is type(Path())else getattr(runtime,name)
        for name in names},sort_keys=True,separators=(',',':')).encode()


def probe(runtime_plan,runtime_plan_sha256,source,runtime_raw):
    value=json.loads(runtime_raw)
    if type(value)is not dict or set(value)!={'python','python_sha256','java_home','java_sha256',
            'worker_revision','worker_inventory_sha256','sdk_inventory_sha256','jars','work',
            'timeout_seconds','maximum_stream_bytes','maximum_evidence_bytes'}:
        raise ValueError('Closed current runtime required')
    runtime=GraphFramesRuntimeConfig(*(Path(value[n])if n in ('python','java_home','jars','work')else value[n]
        for n in ('python','python_sha256','java_home','java_sha256','worker_revision',
            'worker_inventory_sha256','sdk_inventory_sha256','jars','work','timeout_seconds',
            'maximum_stream_bytes','maximum_evidence_bytes')))
    if runtime_bytes(runtime)!=runtime_raw:raise ValueError('Canonical exact runtime required')
    selection=GraphFramesLocalRuntimeSelection(source,runtime_plan,runtime_plan_sha256)
    policy=LocalGraphFramesRuntimePolicy(selection)
    inventory=policy.admit_graphframes_runtime(runtime,policy)
    from graphframes.classic.graphframe import GraphFrame
    import importlib.metadata
    if GraphFrame.__module__!='graphframes.classic.graphframe':raise ValueError('Actual GraphFrame constructor required')
    policy.renew_graphframes_runtime(inventory,runtime,policy)
    return {'profile':'ashlar-current-isolated-graphframes-runtime/0.1',
        'runtime_sha256':hashlib.sha256(runtime_raw).hexdigest(),'runtime_plan_sha256':runtime_plan_sha256,
        'python':str(Path(sys.executable)),'worker_revision':inventory.worker_revision,
        'versions':VERSIONS,'constructor':GraphFrame.__module__+'.'+GraphFrame.__name__,
        'numpy_version':importlib.metadata.version('numpy')}


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('runtime-plan','runtime-plan-sha256','source','runtime'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args()
    answer=probe(Path(args.runtime_plan),args.runtime_plan_sha256,Path(args.source),snapshot(Path(args.runtime),65536))
    print(json.dumps(answer,sort_keys=True,separators=(',',':')))
