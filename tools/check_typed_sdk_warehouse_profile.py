"""Read-only original managed profile gate on an explicitly dedicated warehouse.

This does not qualify a changed build by editing its expected fingerprints.
Original native responses are retained, and the dedicated compute is stopped.
"""
from pathlib import Path
import argparse,json,subprocess
from databricks_transport import sql_result
from weft_native_profile import ENGINE_SQL,ANSI_SQL,EXPECTED_PROFILE,verify_observations
from ashlar.publication import ResolutionError

def main():
 parser=argparse.ArgumentParser()
 parser.add_argument('--profile',required=True)
 parser.add_argument('--warehouse',required=True)
 parser.add_argument('--output',type=Path,required=True)
 args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=False)
 def cli(*command):
  result=subprocess.run(['databricks',*command,'--profile',args.profile],text=True,capture_output=True,check=True)
  return json.loads(result.stdout)if result.stdout.strip()else None
 warehouse=cli('warehouses','get',args.warehouse,'-o','json')
 if warehouse['name']!='umf-typed-sdk-qualification-20261016' or warehouse['cluster_size']!='2X-Small' or warehouse['min_num_clusters']!=1 or warehouse['max_num_clusters']!=1 or warehouse['auto_stop_mins']!=5:
  raise ValueError('Explicit approved dedicated warehouse configuration required')
 (args.output/'warehouse.json').write_text(json.dumps(warehouse,indent=2)+'\n')
 report={'warehouse':args.warehouse,'temporal':'HELD','scope':'actual native engine/settings admission; no data or action qualification'}
 try:
  responses=[]
  for label,sql in [('engine',ENGINE_SQL),('ansi',ANSI_SQL)]:
   response=cli('api','post','/api/2.0/sql/statements','--json',json.dumps({'warehouse_id':args.warehouse,'statement':sql,'wait_timeout':'50s','row_limit':10}))
   (args.output/(label+'.json')).write_text(json.dumps(response,indent=2)+'\n')
   responses.append(sql_result(response))
  try:verify_observations(EXPECTED_PROFILE,*responses)
  except ResolutionError as error:
   report.update(status='profile-refused',reason=str(error))
  else:report.update(status='profile-admitted')
 finally:
  cli('warehouses','stop',args.warehouse,'--no-wait')
  report['stopRequested']=True
  (args.output/'verification.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report))
if __name__=='__main__':main()
