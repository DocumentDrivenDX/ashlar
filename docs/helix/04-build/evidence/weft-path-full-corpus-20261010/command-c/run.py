"""Reviewed corpus execution wrapper; no authority from same-run equality."""
import argparse,hashlib,importlib.util,json,pathlib,sys

def verify(d):
 p=pathlib.Path(d['path'])
 with p.open('rb') as f:b=f.read(d['bytes']+1)
 if len(b)!=d['bytes'] or hashlib.sha256(b).hexdigest()!=d['sha256']:raise ValueError('resource-custody')

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--command',required=True);parser.add_argument('--sha256',required=True);a=parser.parse_args()
 with open(a.command,'rb') as f:raw=f.read(1000001)
 if len(raw)>1000000 or hashlib.sha256(raw).hexdigest()!=a.sha256:raise ValueError('command-pin')
 plan=json.loads(raw)
 for d in plan['resources']:verify(d)
 spec=importlib.util.spec_from_file_location('reviewed_full_paths_corpus',plan['producer']);m=importlib.util.module_from_spec(spec);sys.modules[spec.name]=m;spec.loader.exec_module(m)
 primary=None;closed=False
 try:
  sys.argv=[plan['producer'],*plan['producerArguments']];m.main()
 except BaseException as e:primary=e
 finally:
  try:
   for d in plan['resources']:verify(d)
   closed=True
  except BaseException as e:
   if primary is None:primary=e
  try:
   payload=(json.dumps({'commandSha256':a.sha256,'closingCustody':closed,'completed':primary is None,'status':'Producer execution custody only; semantic acceptance/index/native qualification are separate'},sort_keys=True)+'\n').encode()
   with pathlib.Path(plan['closingReceipt']).open('xb') as f:f.write(payload)
  except BaseException as e:
   if primary is None:primary=e
   else:
    try:primary.cleanup_failed=True
    except BaseException:pass
 if primary is not None:raise primary
if __name__=='__main__':main()
