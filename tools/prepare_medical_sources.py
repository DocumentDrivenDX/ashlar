"""Copy exact current/historical medical inputs; no semantic/native admission."""
import argparse,hashlib,json,subprocess,zipfile
from pathlib import Path
PIN='1f7b5f5d2a355c4b476e3a96b289b9048f03f567'
HISTORICAL='b36c504b130932987efaaa377cdeecc633a745d3'
ROOT=Path(__file__).resolve().parents[1]
def sha(raw):return hashlib.sha256(raw).hexdigest()
def prepare(repo,output):
    for args,wanted in [(['rev-parse','HEAD'],PIN),(['status','--porcelain'],'')]:
        if subprocess.check_output(['git','-C',str(repo),*args],text=True).strip()!=wanted:raise ValueError('Exact clean original pack source required')
    corpus=json.loads((ROOT/'docs/helix/03-test/fixtures/domain-pack-corpus.json').read_bytes());pack=next(p for p in corpus['packs']if p['id']=='medical')
    if output.exists():raise ValueError('Fresh medical source directory required')
    pending={};custody=[]
    for entry in pack['files']:
        raw=(repo/pack['root']/entry['path']).read_bytes()
        if sha(raw)!=entry['sha256']or len(raw)!=entry['bytes']:raise ValueError('Original current medical bytes differ')
        name='upstream/'+entry['path'];pending[name]=raw;custody.append({'path':name,'bytes':len(raw),'sha256':sha(raw),'origin':'current-pack-1.1.0'})
    graph=pack['graph'];archive=(repo/graph['archive']['path']).read_bytes()
    if sha(archive)!=graph['archive']['sha256']:raise ValueError('Exact historical medical archive required')
    pending['historical/original-1.0.0.zip']=archive
    with zipfile.ZipFile(repo/graph['archive']['path'])as zipped:
        if len(zipped.namelist())!=len(set(zipped.namelist()))or set(zipped.namelist())!={e['member']for e in graph['historicalInputs']}:raise ValueError('Complete original archive member inventory required')
        for entry in graph['historicalInputs']:
            member=Path(entry['member'])
            if member.is_absolute()or '..'in member.parts:raise ValueError('Nonlocal original archive member')
            raw=zipped.read(entry['member'])
            if sha(raw)!=entry['sha256']or len(raw)!=entry['bytes']:raise ValueError('Historical original archive bytes differ')
            name='historical/archive/'+entry['member'];pending[name]=raw;custody.append({'path':name,'bytes':len(raw),'sha256':sha(raw),'origin':'historical-pack-1.0.0'})
    original=subprocess.check_output(['git','-C',str(repo),'show',HISTORICAL+':spec/domain-packs/medical/pack.json'])
    if sha(original)!=graph['sourcePackSha256']or json.loads(original)['version']!='1.0.0':raise ValueError('Original historical pack does not match historical graph')
    pending['historical/original-pack.json']=original
    for name,raw in pending.items():
        path=output/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
    receipt={'format':'ashlar-medical-original-source-custody/0.1','packRevision':PIN,'historicalPackRevision':HISTORICAL,'currentPackSha256':sha(pending['upstream/pack.json']),'historicalPackSha256':sha(original),'historicalArchiveSha256':sha(archive),'files':custody,'qualification':'Original current1.1 data and historical1.0 archive/own model remain separate. Identical model bytes are observed, never inferred. No retrieval, clinical validation, semantic admission or native execution.'}
    (output/'source-custody.json').write_text(json.dumps(receipt,indent=2)+'\n');return receipt
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();prepare(a.source,a.output)
