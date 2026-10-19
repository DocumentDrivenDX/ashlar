# SPDX-License-Identifier: MIT OR Apache-2.0
"""Two authored STRING cases; original RECORD rows, not replacement graph edges.

Preparation is not activation authority. Actual current release/source owners
remain mandatory; the other fifteen authored cases are not selected here.
"""
import base64,hashlib,json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from ashlar.graph_release import GraphRelease,release_columns,validate_release_carrier
from .finite_pack import FinitePackDefinition
from .finite_dataset import FinitePackSource
from .finite_projection import original_finite_pack_oracle
from .truss_preparation import json_bytes
from .resources import RESOURCE_ROOT
from .schema_rows import fixture_columns
from .supply_chain_request import source_snapshot
from .lifecycle import owned_context,finish

PROFILE='ashlar-puppy-finite-pack-strings/0.1'
CASES={'archaeology':('missing-media','assets',('id','availability')),
       'ecology':('network','network_links',('upstream_id','downstream_id'))}

def bag(rows):return Counter(json.dumps(row,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False)for row in rows)

@dataclass(frozen=True)
class PuppyFinitePackConfiguration:
    definition:FinitePackDefinition
    model:bytes
    graph:bytes
    pack:bytes
    release:bytes
    release_sha256:str


def project_finite_pack_strings(config):
    """Independent original bytes to reversible STRING projection and exact oracle."""
    if type(config)is not PuppyFinitePackConfiguration or type(config.definition)is not FinitePackDefinition or config.definition.name not in CASES:raise ValueError('Closed archaeology/ecology STRING selector required')
    if any(type(raw)is not bytes or not 0<len(raw)<=16*1024*1024 for raw in (config.model,config.graph,config.pack,config.release)):raise ValueError('Original bounded byte carriers required')
    definition=config.definition;facts=definition.inputs(config.model,config.graph,config.pack)
    if type(config.release_sha256)is not str or hashlib.sha256(config.release).hexdigest()!=config.release_sha256:raise ValueError('Original release byte digest differs')
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise ValueError('Duplicate original release member')
            value[key]=item
        return value
    def refuse(_):raise ValueError('Exact original release scalars required')
    release=json.loads(config.release,object_pairs_hook=pairs,parse_float=refuse,parse_constant=refuse)
    if type(release)is not dict or release.get('format')!='ashlar-graph-release/0.1':raise ValueError('Named original graph release required')
    batch,bindings=definition.build(config.model,config.graph)
    expected=original_finite_pack_oracle(definition,config.model,config.graph,bindings,batch,fixture_columns(RESOURCE_ROOT))
    for kind,role in (('node','object_current'),('edge','edge_current')):
        if bag(release[kind+'s'])!=bag(validate_release_carrier(row,kind)for row in expected[role]):raise ValueError('Complete independent canonical carrier bag differs')
    case,record,fields=CASES[definition.name];cases=dict(definition.cases(config.model,config.graph,config.pack))
    if case not in cases:raise ValueError('Original selected scenario absent')
    model=json.loads(config.model);graph=json.loads(config.graph)
    definitions={(module['id'],element['id']):element for module in model['modules']for element in module['elements']}
    selected=definitions[('domain',record)]
    members={definitions[(ref['module'],ref['element'])]['name']:ref['element']for ref in selected['members']}
    for name in fields:
        if definitions[('domain',members[name])].get('scalarType')!='string':raise ValueError('Original authored STRING field required')
    assigned={(item['type_id'],item['id']):item['originalKey']for item in bindings['entities']if item['kind']=='object'}
    originals={obj['key']:obj for obj in graph['objects']};nodes=[]
    for row in release['nodes']:
        obj=originals[assigned[(row['type_id'],row['id'])]]
        entry=dict(row,original_record=obj['type']['element'],selected_0=None,selected_1=None)
        if obj['type']['element']==record:
            for index,name in enumerate(fields):
                token=obj['values'][members[name]]
                if type(token)is not str:raise ValueError('Complete selected non-null STRING value required')
                entry['selected_'+str(index)]=token
        nodes.append(entry)
    oracle=definition.oracle(config.model,config.graph)['scenarios'][case]
    query=("MATCH (n:FinitePackNode) WHERE n.original_record = 'assets' AND n.selected_1 = 'external' RETURN n.selected_0 AS result_0"if definition.name=='archaeology'else
           "MATCH (n:FinitePackNode) WHERE n.original_record = 'network_links' RETURN n.selected_0 AS result_0,n.selected_1 AS result_1")
    return {'profile':PROFILE,'pack':definition.name,'case':case,'original_sql':cases[case],'query':query,
            'original_facts':facts,'release_sha256':config.release_sha256,'original_release_base64':base64.b64encode(config.release).decode(),'original_model_base64':base64.b64encode(config.model).decode(),'original_graph_base64':base64.b64encode(config.graph).decode(),'original_pack_base64':base64.b64encode(config.pack).decode(),'nodes':nodes,'edges':release['edges'],
            'expected':[list(row)for row in oracle],'result_columns':['result_0']if definition.name=='archaeology'else['result_0','result_1'],'field_ids':[members[name]for name in fields],
            'scope':'Two fixed authored STRING cases only; no other fifteen scenario or native activation qualification.'}


def _renew(source,config,policy,context,original):
    if type(source)is not FinitePackSource or source.definition!=config.definition or source.model!=config.model or source.graph!=config.graph:raise ValueError('Actual original finite source owner required')
    if source_snapshot(source.metadata())!=original:raise ValueError('Original admitted source changed')
    if policy.admit_original_release(GraphRelease(config.release,config.release_sha256),context)is not None:raise PermissionError('Actual current original release admission required')


def prepare_finite_pack_strings(config,output,*,source,policy,context):
    """Prepare complete STRING tables and Puppy model under current held owners."""
    projection=project_finite_pack_strings(config);output=Path(output)
    if not output.is_absolute()or output.parent.resolve()!=output.parent or output.exists():raise ValueError('Fresh canonical carrier directory required')
    if type(source)is not FinitePackSource:raise ValueError('Actual original finite source required')
    original=source_snapshot(source.metadata())
    with owned_context(policy.hold(context)):
        _renew(source,config,policy,context,original)
        import duckdb
        output.mkdir(mode=0o700);database=output/'carrier.duckdb';connection=None;primary=None;committed=False
        try:
            connection=duckdb.connect(str(database));connection.execute("SET threads=1");connection.execute("SET memory_limit='64MB'")
            connection.execute('BEGIN TRANSACTION');connection.execute('CREATE SCHEMA finite_projection')
            model={'catalogs':[{'name':'finite_pack','type':'duckdb','jdbc':{'jdbcUri':'jdbc:duckdb:'+str(database)}}],'node':[],'edge':[]}
            for kind,role,label in (('node','nodes','FinitePackNode'),('edge','edges','FinitePackEdge')):
                names=list(release_columns(kind))+(['original_record','selected_0','selected_1']if kind=='node'else [])
                rows=projection[role]
                connection.execute('CREATE TABLE finite_projection.'+role+'('+','.join('"'+name+'" VARCHAR'for name in names)+',carrier_key VARCHAR,carrier_id VARCHAR)')
                if rows:connection.executemany('INSERT INTO finite_projection.'+role+' VALUES('+','.join('?'for _ in names+['carrier_key','carrier_id'])+')',[[row[name]for name in names]+[row['graph_id'],row['id']]for row in rows])
                readback=[dict(zip(names,row))for row in connection.execute('SELECT '+','.join('"'+name+'"'for name in names)+' FROM finite_projection.'+role).fetchall()]
                if bag(readback)!=bag(rows):raise ValueError('Complete prepared original STRING carrier bag differs')
                attrs=[name for name in names if name not in ('id','graph_id')]+['carrier_key','carrier_id'];key=kind+'_key';entry={'label':label,'dataSourceGroup':{'externalDataSource':{'enabled':True,'catalog':'finite_pack','schema':'finite_projection','table':role,'mappedField':[{'sourceFieldName':name,'targetFieldName':name}for name in attrs]+[{'sourceFieldName':'graph_id','targetFieldName':key}]}},'id':[{'name':key,'type':'STRING'}],'attribute':[{'name':name,'type':'STRING'}for name in attrs]}
                if kind=='edge':entry.update(fromNodeLabel='FinitePackNode',toNodeLabel='FinitePackNode',fromKey=[{'name':'src','type':'STRING'}],toKey=[{'name':'dst','type':'STRING'}])
                model[kind].append(entry)
            _renew(source,config,policy,context,original);connection.execute('COMMIT');committed=True
        except BaseException as error:primary=error
        finish(primary,([lambda:connection.execute('ROLLBACK')if not committed else None,connection.close]if connection is not None else [])+[lambda:_renew(source,config,policy,context,original)])
        for name,value in (('model.json',model),('receipt.json',projection)):
            with (output/name).open('x',encoding='utf8')as stream:json.dump(value,stream,ensure_ascii=False,allow_nan=False)
            (output/name).chmod(0o444)
        database.chmod(0o444)
    return projection


def execute_finite_pack_strings(config,*,source,transport,policy,context,observe,maximum_rows=1000,maximum_bytes=1048576):
    """Actual Cypher observations; owner binds the selected prepared native model."""
    projection=project_finite_pack_strings(config)
    if type(source)is not FinitePackSource or type(maximum_rows)is not int or not 0<maximum_rows<=10000 or type(maximum_bytes)is not int or not 0<maximum_bytes<=16777216 or not callable(observe):raise ValueError('Actual finite owner and explicit bounded observation required')
    original=source_snapshot(source.metadata());primary=None;result=None
    with owned_context(policy.hold(context)):
        def query(case,script):
            raw=transport.query('Cypher',script,context)
            json_bytes(raw,maximum_bytes)
            rows=source_snapshot(raw)
            if type(rows)is not list or len(rows)>maximum_rows:raise ValueError('Complete bounded native rows required')
            observe({'case':case,'query':script,'native_rows':rows});return rows
        def native_role(kind,role,label):
            names=list(release_columns(kind))+(['original_record','selected_0','selected_1']if kind=='node'else [])
            pattern='(n:'+label+')'if kind=='node'else'(s:FinitePackNode)-[n:'+label+']->(t:FinitePackNode)'
            column=lambda name:'carrier_key'if name=='graph_id'else'carrier_id'if name=='id'else name
            rows=query('full-'+role,'MATCH '+pattern+' RETURN '+','.join('n.'+column(name)+' AS '+name for name in names)+',id(n) AS native_id'+(',id(s) AS native_src,id(t) AS native_dst'if kind=='edge'else''))
            actual=[]
            for row in rows:
                if row['native_id']!=label+'['+row['graph_id']+']'or(kind=='edge'and(row['native_src']!='FinitePackNode['+row['src']+']'or row['native_dst']!='FinitePackNode['+row['dst']+']')):raise ValueError('Actual native identity/incidence differs')
                if set(row)!=set(names)|{'native_id'}|({'native_src','native_dst'}if kind=='edge'else set()):raise ValueError('Complete native carrier columns required')
                actual.append({name:row[name]for name in names})
            if bag(actual)!=bag(projection[role]):raise ValueError('Full native original canonical/projected bag differs')
        def full_native():
            finish(None,[lambda:native_role('node','nodes','FinitePackNode'),lambda:native_role('edge','edges','FinitePackEdge')])
        try:
            _renew(source,config,policy,context,original)
            full_native()
            rows=query(projection['case'],projection['query']);names=projection['result_columns']
            if any(set(row)!=set(names)or any(type(row[name])is not str for name in names)for row in rows):raise ValueError('Exact complete native STRING projection required')
            actual=[[row[name]for name in names]for row in rows]
            if bag(actual)!=bag(projection['expected']):raise ValueError('Independent original authored scenario bag differs')
            result={'pack':projection['pack'],'case':projection['case'],'original_sql':projection['original_sql'],'rows':actual,'scope':projection['scope']}
        except BaseException as error:primary=error
        finish(primary,[lambda:_renew(source,config,policy,context,original),full_native])
    return result
