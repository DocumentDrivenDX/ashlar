"""Finite native beta GQL checks of two explicitly pinned frozen release fixtures."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import subprocess
import shutil
import urllib.error
import urllib.request
from run_graph_release_graphframes import load_release, columns, oracle

LIMIT = 1048576

def identifier(value):
    if type(value) is not str or not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', value):
        raise ValueError('Canonical lowercase UUID required')
    return value

def bag(rows):
    return Counter(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False) for row in rows)

def cases(value):
    result = []
    for kind, role, pattern in [('node','nodes','(n:AshlarNode)'),('edge','edges','()-[n:AshlarEdge]->()')]:
        names = columns(kind)
        result.append((role, 'MATCH '+pattern+' RETURN '+', '.join('n.'+n+' AS '+n for n in names), value[role], names))
    endpoints = [dict(source=e['src'], relationship_id=e['graph_id'], destination=e['dst']) for e in value['edges']]
    result.append(('endpoints', 'MATCH (s:AshlarNode)-[e:AshlarEdge]->(d:AshlarNode) RETURN s.graph_id AS source, e.graph_id AS relationship_id, d.graph_id AS destination', endpoints, ('source','relationship_id','destination')))
    hops = [dict(source=a['src'], first_edge=a['graph_id'], middle=a['dst'], second_edge=b['graph_id'], destination=b['dst']) for a in value['edges'] for b in value['edges'] if a['dst']==b['src'] and a['graph_id']!=b['graph_id']]
    result.append(('two-hop-distinct-edges', 'MATCH (s:AshlarNode)-[a:AshlarEdge]->(m:AshlarNode)-[b:AshlarEdge]->(d:AshlarNode) WHERE a.graph_id <> b.graph_id RETURN s.graph_id AS source, a.graph_id AS first_edge, m.graph_id AS middle, b.graph_id AS second_edge, d.graph_id AS destination', hops, ('source','first_edge','middle','second_edge','destination')))
    walks = [dict(source=a['src'], first_edge=a['graph_id'], middle=a['dst'], second_edge=b['graph_id'], destination=b['dst']) for a in value['edges'] for b in value['edges'] if a['dst']==b['src']]
    result.append(('two-hop-walk', 'MATCH WALK (s:AshlarNode)-[a:AshlarEdge]->(m:AshlarNode)-[b:AshlarEdge]->(d:AshlarNode) RETURN s.graph_id AS source, a.graph_id AS first_edge, m.graph_id AS middle, b.graph_id AS second_edge, d.graph_id AS destination', walks, ('source','first_edge','middle','second_edge','destination')))
    counts=oracle(value)
    for name, pattern in [('nodes','(n:AshlarNode)'),('edges','()-[n:AshlarEdge]->()')]:
        result.append((name+'-count','MATCH '+pattern+' RETURN count(n) AS row_count',[{'row_count':counts[name]}],('row_count',)))
    return result

def verify(body, expected, names):
    if type(body) is not dict or body.get('status',{}).get('code')!='00000' or body['status'].get('cause') is not None or body.get('additionalStatuses') not in (None,[]):raise ValueError('Native successful terminal warning-free GQL status required')
    result=body.get('result',{})
    if result.get('kind')!='TABLE' or body.get('nextPage') is not None or result.get('nextPage') is not None:raise ValueError('One bounded complete scalar table required')
    cols=result.get('columns');rows=result.get('data')
    if type(cols) is not list or [c.get('name') for c in cols]!=list(names) or type(rows) is not list:raise ValueError('Exact scalar column inventory required')
    count=list(names)==['row_count']
    if any(c.get('gqlType') not in (('INT64','UINT64') if count else ('STRING',)) for c in cols):raise ValueError('Exact native scalar types required')
    if any(type(row) is not dict or set(row)!=set(names) or any((type(v) is not int if count else v is not None and type(v) is not str) for v in row.values()) for row in rows):raise ValueError('Exact scalar carriers required')
    if bag(rows)!=bag(expected):raise ValueError('Original full row bag differs')

def _object(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise ValueError('Duplicate native JSON member')
        result[key]=value
    return result

def request(workspace, graph, query, token):
    req=urllib.request.Request('https://api.fabric.microsoft.com/v1/workspaces/'+workspace+'/graphModels/'+graph+'/executeQuery?beta=true',data=json.dumps({'query':query}).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=60) as response:status=response.status;raw=response.read(LIMIT+1)
    except urllib.error.HTTPError as error:
        try:status=error.code;raw=error.read(LIMIT+1)
        finally:error.close()
    if len(raw)>LIMIT:raise ValueError('Native response exceeds finite 1 MiB bound')
    return status,json.loads(raw.decode('utf-8'),object_pairs_hook=_object,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite native JSON')))

def check(inputs, workspace, graphs, output, *, transport=None, token=None):
    identifier(workspace)
    if set(inputs)!=set(graphs) or set(inputs)!={'R1','R2'}:raise ValueError('Two explicit releases required')
    for graph in graphs.values():identifier(graph)
    if graphs['R1']==graphs['R2']:raise ValueError('Distinct native graph handles required')
    output=Path(output)
    if output.exists():raise ValueError('Fresh output required')
    releases={name:load_release(Path(path).read_bytes(),digest) for name,(path,digest) in inputs.items()}
    report={'profile':'ashlar-fabric-release-query/0.1','workspace':workspace,'graphs':graphs,'releases':{n:d for n,(_,d) in inputs.items()},'records':[],'allPassed':False,'qualification':'Actual frozen synthetic release fixtures only; no public reader, production publication authority, retained source custody or continuous refresh guarantee. Distinct-edge two-hop and explicit WALK repeatable-edge semantics are checked separately; native results, not defaults, determine qualification.'}
    # Reserve output before authentication or any native submission; retain partial failures.
    with output.open('x',encoding='utf-8') as stream:stream.write(json.dumps(report,indent=2)+'\n')
    try:
        if transport is None:
            azure_cli=shutil.which('az')
            if azure_cli is None:raise ValueError('Azure CLI executable unavailable on PATH')
            auth=subprocess.run([azure_cli,'account','get-access-token','--resource','https://api.fabric.microsoft.com','--output','json'],capture_output=True,timeout=30)
            if auth.returncode:raise ValueError('Existing Azure CLI authentication unavailable')
            token=json.loads(auth.stdout)['accessToken'];transport=request
        scheduled=[(n,c) for n in ('R1','R2') for c in cases(releases[n])]+[('R1',('after-r2-'+c[0],)+c[1:]) for c in cases(releases['R1'])[:2]]
        for name,(case,query,expected,names) in scheduled:
            record={'release':name,'case':case,'query':query,'expected':expected};report['records'].append(record)
            status,body=transport(workspace,graphs[name],query,token);record.update(httpStatus=status,response=body)
            if status!=200:raise ValueError('Native HTTP failure')
            verify(body,expected,names);record['passed']=True
            output.write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
        report['allPassed']=True
    except BaseException as error:
        report['failureType']=type(error).__name__
        raise
    finally:output.write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    for n in ('r1','r2'):
        parser.add_argument('--'+n,type=Path,required=True);parser.add_argument('--'+n+'-sha256',required=True);parser.add_argument('--'+n+'-graph',required=True)
    parser.add_argument('--workspace',required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();check({'R1':(args.r1,args.r1_sha256),'R2':(args.r2,args.r2_sha256)},args.workspace,{'R1':args.r1_graph,'R2':args.r2_graph},args.output)
    print('Passed two explicit Fabric release fixtures and R1 after R2')
