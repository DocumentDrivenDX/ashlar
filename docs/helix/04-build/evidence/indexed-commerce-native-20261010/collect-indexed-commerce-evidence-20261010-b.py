"""After-terminal byte custody only; never launches engines or opens PostgreSQL.
Independent native-cell/ACK semantic review remains required separately.
"""
import argparse,gzip,hashlib,json,tarfile
from pathlib import Path

def sha(raw):return hashlib.sha256(raw).hexdigest()
def record(path):
    raw=path.read_bytes();return {'path':str(path),'sha256':sha(raw),'bytes':len(raw)}
def main():
    p=argparse.ArgumentParser();p.add_argument('--command',type=Path,required=True);p.add_argument('--publication',type=Path,required=True);p.add_argument('--queries',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if a.output.exists():raise ValueError('Fresh evidence directory required')
    command=json.loads(a.command.read_bytes())
    # Root must separately attest both processes terminal/stopped before invoking.
    for root in [a.publication,a.queries]:
        if not (root/'report.json').is_file():raise ValueError('Complete terminal report required')
        if root.is_symlink():raise ValueError('Original root required')
    resources={}
    for ref in command['references']:
        f=Path(ref['path']);raw=f.read_bytes()
        if sha(raw)!=ref['sha256'] or len(raw)!=ref['bytes']:raise ValueError('Frozen command reference differs')
        value=json.loads(raw)
        for key in ['files','selected_runtime_resources','additional_resources','selected_files','setup_jars','query_jars','new_delta4_resources','typing_resources','loaded_modules']:
            for item in value.get(key,[]):
                path=item['path']
                if not path.startswith('/'):path=str(Path(value['root'])/path)
                if path in resources and resources[path]!=item['sha256']:raise ValueError('Conflicting resource custody')
                resources[path]=item['sha256']
                if 'original' in item:
                    if sha(Path(item['original']).read_bytes())!=item['sha256'] or (Path(path).stat().st_mode&0o777)!=int(item['mode'],8):raise ValueError('Originalcopiedresource/mode differs')
    for name,digest in resources.items():
        if sha(Path(name).read_bytes())!=digest:raise ValueError('Closing original resource changed: '+name)
    a.output.mkdir()
    copies=[]
    for label,root in [('publication',a.publication),('queries',a.queries)]:
        files=sorted(f for f in root.rglob('*') if f.is_file())
        if any(f.is_symlink() for f in files):raise ValueError('Native symlink unsupported')
        inventory=[record(f)for f in files]
        report=(root/'report.json').read_bytes();(a.output/(label+'-report.json.gz')).write_bytes(gzip.compress(report,mtime=0))
        with tarfile.open(a.output/(label+'-artifacts.tar.gz'),'w:gz')as archive:
            for f in files:archive.add(f,arcname=label+'/'+str(f.relative_to(root)),recursive=False)
        (a.output/(label+'-original-files.json')).write_text(json.dumps(inventory,indent=2)+'\n')
        log=Path(str(root)+'.log')
        if not log.is_file():raise ValueError('Original terminal log required')
        (a.output/(label+'.log')).write_bytes(log.read_bytes())
    (a.output/'command.json').write_bytes(a.command.read_bytes())
    for i,ref in enumerate(command['references']):(a.output/('reference-'+str(i)+'.json')).write_bytes(Path(ref['path']).read_bytes())
    for f in sorted(a.output.iterdir()):copies.append(record(f))
    (a.output/'custody.json').write_text(json.dumps({'format':'ashlar-indexed-commerce-after-terminal-custody/0.1','files':copies,'closing_resource_files':len(resources),'qualification':'Exact lossless terminal artifacts and closing file custody only; process stop, native full-row/source oracle, PG ACK and compiler/guard semantics need independent review. Historical runtime inventory pending wording is overridden by final command snapshot.'},indent=2)+'\n')
    print(record(a.output/'custody.json'))
if __name__=='__main__':main()
