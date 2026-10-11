"""Actual finite exact commerce scenario arithmetic; never Gremlin math/DOUBLE."""
import argparse,base64,hashlib,json,os,subprocess
from pathlib import Path
from prepare_commerce_exact_puppygraph import projection,PROFILE_EXACT
from run_commerce_release_graphframes import original,bag
from run_graph_release_graphframes import load_release,row_bag
from private_graph_custody import PROFILE
from check_puppygraph_releases import native_text

def admitted(release,custody,rs,cs,prepared,prepared_sha):
    if hashlib.sha256(prepared).hexdigest()!=prepared_sha:raise ValueError('Trusted exact adapter receipt bytes differ')
    p=json.loads(prepared)
    if p['format']!=PROFILE_EXACT or p['release_sha256']!=rs or p['custody_sha256']!=cs:raise ValueError('Original adapter custody differs')
    value=load_release(release,rs,custody_profile=PROFILE,custody_payload=custody,trusted_custody_sha256=cs);graph,nodes,edges,a=original(value);model=json.loads(base64.b64decode(a['original_model_base64']))
    projected=projection(value,graph,nodes,edges,model)
    if projected!=(p['nodes'],p['edges'],p['numeric_input_guards'],p['all_unfiltered_intermediate_guards']):raise ValueError('Every source/representability/intermediate guard must precede query acquisition')
    return p

def native_custody(prepared,language,endpoint):
    expected_endpoint='bolt://127.0.0.1:17887'if language=='Cypher'else'ws://127.0.0.1:18182/gremlin'
    if endpoint!=expected_endpoint:raise ValueError('Explicit dedicated local native route required')
    container='ashlar-release-puppy113'
    result=subprocess.run(['docker','inspect',container],check=True,capture_output=True,text=True)
    facts=json.loads(result.stdout)[0];ports=facts['HostConfig']['PortBindings']
    expected_ports={'7687/tcp':[{'HostIp':'127.0.0.1','HostPort':'17887'}],'8081/tcp':[{'HostIp':'127.0.0.1','HostPort':'18881'}],'8182/tcp':[{'HostIp':'127.0.0.1','HostPort':'18182'}]}
    image='sha256:6d016d5a7e3eab0a57bd4d23c4e218467d00c7661fcda215ea2a429941a68cee'
    if not isinstance(facts.get('Id'),str) or not facts['Id'] or facts['Name']!='/'+container or facts['Image']!=image or ports!=expected_ports or facts['State']['Running']is not True or facts['HostConfig']['Memory']!=3221225472 or facts['HostConfig']['NanoCpus']!=2000000000:raise ValueError('Dedicated native instance/image/loopback binding differs')
    hashes=subprocess.run(['docker','exec',container,'sha256sum','/tmp/releases.duckdb','/tmp/commerce.duckdb','/tmp/augmentations.duckdb','/tmp/commerce-exact.duckdb'],check=True,capture_output=True,text=True)
    observed={line.split()[1]:line.split()[0]for line in hashes.stdout.splitlines()}
    expected={'/tmp/releases.duckdb':'5d69ce5c13549053e3691e689e3f135f09baf7427b9d1d93ee7a42d8aa9ed989','/tmp/commerce.duckdb':'f9c4e8d4ec7edf3c6943eb6187db6d1a54f0ba85e480988ee4e8cdfd229278e4','/tmp/augmentations.duckdb':'926e9de45d0a0409a6a9be88e933b63332d81fa2316e6a19f0303700eafdfb71','/tmp/commerce-exact.duckdb':prepared['database_sha256']}
    if observed!=expected:raise ValueError('Complete fixture native sealed database inventory differs')
    return {'container_id':facts['Id'],'container':container,'image':image,'ports':ports,'sealed_databases':observed,'memory_bytes':facts['HostConfig']['Memory'],'nano_cpus':facts['HostConfig']['NanoCpus']}

def scalar_integer(value):
    if not isinstance(value,int)or isinstance(value,bool)or not -(2**63)<=value<2**63:raise ValueError('Exact native signed64 integer result required; float/bool refused')
    return int(value)

def verify_intermediates(raw,prepared,scenario):
    actual=[]
    for row in raw:
        if scenario=='partial-return':actual.append({'scenario':scenario,'source_keys':row['source_keys'],'subtract':str(scalar_integer(row['subtract'])),'add':str(scalar_integer(row['add']))})
        else:actual.append({'scenario':'refund','source_keys':row['source_keys'],'multiply_coefficient':str(scalar_integer(row['multiply_coefficient'])),'scale':2})
    expected=[r for r in prepared['all_unfiltered_intermediate_guards']if r['scenario']==scenario]
    if bag(actual)!=bag(expected):raise ValueError('Unfiltered native exact intermediate bag differs')

def cypher(prepared,endpoint,user,password):
    from neo4j import GraphDatabase
    from ashlar_host.puppy_commerce_scenarios import cypher_queries
    records=[]
    with GraphDatabase.driver(endpoint,auth=(user,password),connection_timeout=5)as driver:
        with driver.session()as session:
            def query(name,q):
                rows=session.run(q).data();records.append({'case':name,'query':q,'native_rows':rows});return rows
            return cypher_queries(prepared,query,records)

def gremlin(prepared,endpoint,user,password):
    from gremlin_python.driver import client,serializer
    from ashlar_host.puppy_commerce_scenarios import gremlin_queries
    remote=client.Client(endpoint,'g',username=user,password=password,message_serializer=serializer.GraphSONSerializersV3d0());records=[]
    def query(name,script):
        rows=remote.submit(script).all().result(timeout=20);records.append({'case':name,'script':script,'native_rows':rows});return rows
    from ashlar_host.lifecycle import finish
    primary=None;result=None
    try:result=gremlin_queries(prepared,query,records)
    except BaseException as error:primary=error
    finish(primary,(remote.close,))
    return result
def execute_checked(prepared,language,endpoint,user,password):
    opening=native_custody(prepared,language,endpoint)
    result=(cypher if language=='Cypher'else gremlin)(prepared,endpoint,user,password)
    closing=native_custody(prepared,language,endpoint)
    if opening!=closing:raise ValueError('Closing native instance/database custody changed; result withheld')
    result['opening_closing_native_custody']=opening
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for n in ('release','custody','prepared','output'):p.add_argument('--'+n,type=Path,required=True)
    for n in ('release-sha256','custody-sha256','prepared-sha256','endpoint'):p.add_argument('--'+n,required=True)
    p.add_argument('--language',choices=['Cypher','Gremlin'],required=True);a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh query receipt required')
    prepared=admitted(a.release.read_bytes(),a.custody.read_bytes(),a.release_sha256,a.custody_sha256,a.prepared.read_bytes(),a.prepared_sha256)
    result=execute_checked(prepared,a.language,a.endpoint,os.environ['ASHLAR_PUPPY_USER'],os.environ['ASHLAR_PUPPY_PASSWORD']);result.update(profile=PROFILE_EXACT,prepared_sha256=a.prepared_sha256,expected=prepared['expected'])
    a.output.write_text(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
