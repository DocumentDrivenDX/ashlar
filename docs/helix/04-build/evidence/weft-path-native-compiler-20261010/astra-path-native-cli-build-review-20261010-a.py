import hashlib,json,pathlib,stat,subprocess
p=pathlib.Path('/private/tmp/weft-paths-native-c6fe2a6-20261010-a');repo=pathlib.Path('/private/tmp/ashlar-weft-distribution-d2d');inv=json.loads((p/'source-inventory.json').read_bytes());summary=json.loads((p/'build-replay-receipt.json').read_bytes())
sha=lambda b:hashlib.sha256(b).hexdigest()
assert sha((p/'source-inventory.json').read_bytes())==summary['sourceInventorySha256']
raw=subprocess.check_output(['git','ls-tree','-rz','--full-tree',inv['sourceCommit']],cwd=repo)
entries={}
for row in raw.split(b'\0'):
 if not row:continue
 metadata,path=row.split(b'\t',1);mode,kind,blob=metadata.decode().split();assert kind=='blob';entries[path.decode()]=(mode,blob)
assert len(inv['files'])==len(entries)==1839
assert {x['path'] for x in inv['files']}==set(entries)
for x in inv['files']:
 q=p/'source'/x['path'];assert not q.is_symlink();b=q.read_bytes();mode,blob=entries[x['path']]
 assert (mode,blob)==(x['mode'],x['gitBlob'])
 assert len(b)==x['bytes'] and sha(b)==x['sha256']
 assert hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()==blob
 assert bool(q.stat().st_mode&0o111)==(mode=='100755')
assert {str(q.relative_to(p/'source')) for q in (p/'source').rglob('*') if q.is_file()}==set(entries)
binary=p/'weft-paths';binarybytes=binary.read_bytes();assert len(binarybytes)==summary['binaryBytes'] and sha(binarybytes)==summary['binarySha256']
assert sha((p/'build.log').read_bytes())==summary['buildLogSha256']
oldreplay=json.loads((p/'native-cli-replay.json').read_bytes());assert sha((p/'native-cli-replay.json').read_bytes())==summary['replaySha256']
fpath=pathlib.Path('/private/tmp/weft-path-lowering-artifacts-20261010-b.jsonl');assert sha(fpath.read_bytes())=='376c29438acc7fc9fd150716f2ff5845b391a1818ae2c029d803a1f134f61ca9'
fixtures=[json.loads(x) for x in fpath.read_text().splitlines()];out=[]
assert len(oldreplay)==len(fixtures)==21
unique_requests=len({json.dumps(x['request'],sort_keys=True) for x in fixtures})
for x,prior in zip(fixtures,oldreplay):
 assert x['test']==prior['test']
 request=json.dumps(x['request'],ensure_ascii=False,separators=(',',':')).encode();assert sha(request)==prior['requestSha256'] and len(request)==prior['requestBytes']
 assert json.loads(prior['stdout'])==x['response'] and prior['stderr']=='' and prior['exit']==0
 assert sha(binary.read_bytes())==summary['binarySha256']
 r=subprocess.run([str(binary)],input=request,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=20,cwd='/private/tmp')
 assert r.returncode==0 and r.stderr==b'' and r.stdout==prior['stdout'].encode() and json.loads(r.stdout)==x['response']
 assert sha(binary.read_bytes())==summary['binarySha256']
 out.append({'test':x['test'],'requestSha256':sha(request),'stdoutSha256':sha(r.stdout),'stdoutBytes':len(r.stdout),'status':x['response']['status'],'exit':r.returncode,'stderrBytes':len(r.stderr)})
# Reobserve all source bytes after all compile calls.
for x in inv['files']:
 b=(p/'source'/x['path']).read_bytes();assert sha(b)==x['sha256'] and len(b)==x['bytes']
result={'sourceCommit':inv['sourceCommit'],'sourceFiles':len(entries),'sourceBytes':sum(x['bytes'] for x in inv['files']),'exactGitBlobModeLengthSha':True,'openingClosingSourceVerified':True,'binary':{'bytes':len(binarybytes),'sha256':sha(binarybytes)},'all21RawStdoutMatch':True,'uniqueRequests':unique_requests,'cases':out,'scope':'Pure compiler execution only; no engine, indexed realization, native SQL or host obligations'}
pathlib.Path('/private/tmp/astra-path-native-cli-build-controls-20261010-a.json').write_text(json.dumps(result,indent=2)+'\n')
print('PASS',len(out),'complete compiler responses;',len(entries),'exact Git source files')
