"""Finite component parity command; external review authorization required."""
import argparse,hashlib,importlib.util,json,pathlib
def cleanup_primary(primary,closing):
 if primary is None:return closing
 if isinstance(primary,Exception) and not isinstance(closing,Exception):return closing
 try:primary.closing_custody_failed=True
 except BaseException:pass
 return primary

def write_receipt(path,result):
 primary=None;stream=None
 try:
  stream=path.open('x');json.dump(result,stream,sort_keys=True,indent=2);stream.write('\n')
 except BaseException as error:primary=error
 finally:
  if stream is not None:
   try:stream.close()
   except BaseException as error:primary=cleanup_primary(primary,error)
 if primary is not None:raise primary

def main():
 p=argparse.ArgumentParser();p.add_argument('--command',required=True,type=pathlib.Path);p.add_argument('--sha256',required=True);p.add_argument('--phase',required=True);p.add_argument('--receipt',required=True,type=pathlib.Path);a=p.parse_args()
 raw=a.command.read_bytes()
 if len(raw)>1000000 or hashlib.sha256(raw).hexdigest()!=a.sha256:raise ValueError('command-pin')
 plan=json.loads(raw)
 def verify():
  for r in plan['resources']:
   path=pathlib.Path(r['path'])
   if path.is_symlink() or not path.is_file() or path.stat().st_size!=r['bytes']:raise ValueError('resource-shape')
   h=hashlib.sha256()
   with path.open('rb') as f:
    for block in iter(lambda:f.read(262144),b''):h.update(block)
   if h.hexdigest()!=r['sha256']:raise ValueError('resource-pin')
 verify()
 spec=importlib.util.spec_from_file_location('bounded_capture',plan['captureModule']);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 c=next(c for c in plan['commands'] if c['phase']==a.phase)
 dynamic=[]
 if a.phase in ('python','browser'):
  base=pathlib.Path(c['stdout']).parent
  receipt=json.loads((base/'cli.json').read_bytes())
  dynamic.append({'path':str(base/'cases.json'),'bytes':receipt['casesBytes'],'sha256':receipt['casesSha256']})
  if a.phase=='python':
   native=next(r for r in plan['resources'] if r['path'].endswith('/weft.abi3.so'))
   dynamic.append(dict(native,path=str(base/'wheel-native/weft.abi3.so')))
  plan['resources'].extend(dynamic)
  verify()
 primary=None;result=None
 try:result=m.capture(c['argv'],c['cwd'],plan['environment'],c['stdout'],c['stderr'],c['timeoutSeconds'],16*1024*1024)
 except BaseException as e:primary=e
 finally:
  try:verify()
  except BaseException as e:
   primary=cleanup_primary(primary,e)
 if primary is not None:raise primary
 result.update({'commandSha256':a.sha256,'phase':a.phase,'openingClosingCustody':True,'environmentInherited':False,'qualification':plan['qualification'],'derivedInputPins':dynamic})
 write_receipt(a.receipt,result)
 if result['exitCode']:raise SystemExit(result['exitCode'])

if __name__=='__main__':main()
