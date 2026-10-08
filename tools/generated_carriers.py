"""Read checked-in UMF-generated installation proposals; refuse stale inputs."""
import hashlib,json,re
from pathlib import Path

TABLES={'object_current','edge_current','source_record','property_journal','tombstone','publication_manifest','publication_attempt_phase','whole_source_history'}
FOLDER='docs/helix/02-design/models/ashlar-delta-runtime/'

def load_generated_carriers(root):
    root=Path(root)
    path=root/'sql/ashlar-delta-v03/runtime-carriers.generated.json'
    original=path.read_bytes();value=json.loads(original)
    if value['profile']!='ashlar-generated-delta-carriers/0.1' or set(value['carriers'])!=TABLES:
        raise ValueError('Complete generated carrier inventory required')
    expected={FOLDER+'index.json'}|{FOLDER+name+'.umf.json' for name in TABLES}
    if set(value['inputs'])!=expected:raise ValueError('Unexpected model input inventory')
    for relative,digest in value['inputs'].items():
        if not isinstance(digest,str) or not re.fullmatch('[0-9a-f]{64}',digest):raise ValueError('Invalid model fingerprint')
        if hashlib.sha256((root/relative).read_bytes()).hexdigest()!=digest:raise ValueError('Stale generated model: '+relative)
    index=json.loads((root/(FOLDER+'index.json')).read_text())
    if set(index['tables'])!=TABLES or len(index['tables'])!=len(TABLES):raise ValueError('Model inventory differs')
    if value['generator']!={'project':'UMF','revision':index['umfRevision'],'extension':'umf.delta.definition/0.1.0'}:
        raise ValueError('Generator provenance differs')
    if not re.fullmatch('[0-9a-f]{40}',index['umfRevision']):raise ValueError('Exact UMF revision required')
    for name,carrier in value['carriers'].items():
        if not carrier['sql'].startswith('CREATE TABLE `'+name+'` (') or not carrier['sql'].endswith(';\n'):
            raise ValueError('Generated target differs')
    return value,hashlib.sha256(original).hexdigest()
