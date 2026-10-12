"""Historical finite-file export correspondence; never protected source ACK authority."""
import base64,hashlib,json,re
from .finite_pack import FinitePackDefinition
from .delta_custody import encoded
PROFILE='ashlar-immutable-finite-file-graph-custody/0.1'
SCOPE='immutable-finite-file-export-bytes-only'

def decode(raw):
    if type(raw)is not bytes or not 0<len(raw)<=16777216:raise ValueError('Finite custody byte bound')
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise ValueError('Duplicate finite custody member')
            value[key]=item
        return value
    def refuse(value):raise ValueError('Exact finite custody JSON required')
    def integer(token):
        if len(token)>20 or not -(2**63)<=int(token)<2**63:raise ValueError('Bounded finite custody integer required')
        return int(token)
    return json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refuse,parse_constant=refuse)


def admit_finite_graph_custody(release,payload,trusted_receipt_sha256,trusted_release_sha256):
    if any(type(value)is not str or re.fullmatch('[0-9a-f]{64}',value)is None for value in (trusted_receipt_sha256,trusted_release_sha256))or type(payload)is not bytes or hashlib.sha256(payload).hexdigest()!=trusted_receipt_sha256:raise ValueError('Trusted finite custody digest differs')
    value=decode(payload)
    keys={'format','scope','releaseSha256','originalManifest','snapshots','roles','source','publicDatasetReceiptBase64','interval','providerInterval'}
    if type(value)is not dict or set(value)!=keys or value['format']!=PROFILE or value['scope']!=SCOPE or (encoded(value)+'\n').encode()!=payload:raise ValueError('Named canonical finite-file custody required')
    if value['releaseSha256']!=trusted_release_sha256 or value['originalManifest']!=release['publication']or value['snapshots']!=release['snapshots']or value['roles']!=release['roles']:raise ValueError('Original finite release/vector differs')
    source=json.loads(release['publication']['validation_report_json'])['source_admission']
    if type(source)is not dict or value['source']!=source:raise ValueError('Original finite source metadata differs')
    facts=FinitePackDefinition(source['name']).facts()
    if encoded({key:source.get(key)for key in facts})!=encoded(facts):raise ValueError('Exact named original finite source facts required')
    if len(value['snapshots'])!=4 or {table.split('.')[-1]for table in value['snapshots']}!={'object_current','edge_current','tombstone','whole_source_history'}or value['roles']['nodes'].split('.')[-1]!='object_current'or value['roles']['edges'].split('.')[-1]!='edge_current':raise ValueError('Complete original finite four-role vector required')
    if json.loads(release['publication']['schema_revisions_json'])!={facts['source_system']:facts['model_sha256']}:raise ValueError('Exact finite manifest source revision required')
    if len(release['nodes'])!=facts['objects']or len(release['edges'])!=facts['edges']:raise ValueError('Complete fixed finite source graph counts required')
    if any(row.get('source_system')!=facts['source_system']or row.get('schema_revision')!=facts['model_sha256']for role in ('nodes','edges')for row in release[role]):raise ValueError('Original finite row source/schema differs')
    if type(value['publicDatasetReceiptBase64'])is not str or not 0<len(value['publicDatasetReceiptBase64'])<=16777216:raise ValueError('Bounded original finite producer receipt required')
    receipt=base64.b64decode(value['publicDatasetReceiptBase64'],validate=True)
    if base64.b64encode(receipt).decode()!=value['publicDatasetReceiptBase64']or hashlib.sha256(receipt).hexdigest()!=source['public_dataset_receipt_sha256']:raise ValueError('Original complete finite producer receipt differs')
    interval=value['interval']
    if type(interval)is not dict or set(interval)!={'opening','closing','wholeVectorHeld','closingSucceeded'}or interval['opening']!=interval['closing']or interval['wholeVectorHeld']is not True or interval['closingSucceeded']is not True:raise ValueError('Original finite full interval did not close')
    expected={'manifest':value['originalManifest'],'snapshots':value['snapshots'],'source':source,'public_dataset_receipt_sha256':hashlib.sha256(receipt).hexdigest()}
    if interval['opening']!=expected:raise ValueError('Original finite interval correspondence differs')
    provider=value['providerInterval']
    if type(provider)is not dict or provider.get('profile')!='ashlar-immutable-file-publication-interval/0.1'or provider.get('publication_id')!=release['publication']['publication_id']or provider.get('manifest_sha256')!=hashlib.sha256(encoded(release['publication']).encode()).hexdigest()or provider.get('source')!=source:raise ValueError('Original released finite provider custody differs')
    return value


def build_finite_graph_custody(release,captured):
    value={'format':PROFILE,'scope':SCOPE,'releaseSha256':release.sha256,'originalManifest':captured['opening']['manifest'],'snapshots':captured['opening']['snapshots'],'roles':json.loads(release.payload)['roles'],'source':captured['opening']['source'],'publicDatasetReceiptBase64':base64.b64encode(captured['producer_receipt']).decode(),'interval':{'opening':captured['opening'],'closing':captured['closing'],'wholeVectorHeld':True,'closingSucceeded':True},'providerInterval':captured['provider_interval']}
    payload=(encoded(value)+'\n').encode()
    admit_finite_graph_custody(json.loads(release.payload),payload,hashlib.sha256(payload).hexdigest(),release.sha256)
    return payload
