"""Retain candidate compiler observations; never creates expected golden bytes."""
import argparse,hashlib,importlib.util,json,pathlib,sys

def read(path,limit):
 with pathlib.Path(path).open('rb') as f:b=f.read(limit+1)
 if len(b)>limit:raise ValueError('input-bound')
 return b

def verify(desc):
 b=read(desc['path'],desc['bytes'])
 if len(b)!=desc['bytes'] or hashlib.sha256(b).hexdigest()!=desc['sha256']:raise ValueError('resource-custody')
 return b

def persist_observation(output, result, primary):
 try:
  payload=(json.dumps(result,sort_keys=True,indent=2)+'\n').encode()
  with (output/'observation.json').open('xb') as f:f.write(payload)
 except BaseException as cleanup_error:
  if primary is None: return cleanup_error
  try: primary.cleanup_failed=True
  except BaseException: pass
 return primary

def main():
 p=argparse.ArgumentParser();p.add_argument('--command',required=True);p.add_argument('--sha256',required=True);a=p.parse_args()
 raw=read(a.command,1000000)
 if hashlib.sha256(raw).hexdigest()!=a.sha256:raise ValueError('command-pin')
 plan=json.loads(raw)
 for d in plan['resources']:verify(d)
 spec=importlib.util.spec_from_file_location('reviewed_paths_transport',plan['producer']);module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
 settings=dict(plan['config'])
 for key in ['source','binary','cases','backend','output','source_inventory','schema_checker']:settings[key]=pathlib.Path(settings[key])
 config=module.Config(**settings);module.verify_source(config)
 output=config.output;output.mkdir(exist_ok=False);records=[];primary=None;closing=False
 try:
  for case in plan['requests']:
   request=verify(case['request']);module.document(request)
   code,out,err=module.transport(config.binary,request,'normal',config)
   stem=output/case['id'];paths={}
   for role,b in [('request',request),('stdout',out),('stderr',err)]:
    path=pathlib.Path(str(stem)+'.'+role)
    with path.open('xb') as f:f.write(b)
    paths[role]={'path':str(path),'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b)}
   records.append({'id':case['id'],'exitCode':code,'artifacts':paths,'status':'candidate-unaccepted-observation'})
 except BaseException as e:primary=e
 finally:
  try:
   for d in plan['resources']:verify(d)
   module.verify_source(config);closing=True
  except BaseException as e:
   if primary is None:primary=e
  result={'status':'candidate-unaccepted-observations; no golden/qualification/native claim','commandSha256':a.sha256,'records':records,'closingCustody':closing,'completed':primary is None and len(records)==9}
  primary=persist_observation(output,result,primary)
 if primary is not None:raise primary
if __name__=='__main__':main()
