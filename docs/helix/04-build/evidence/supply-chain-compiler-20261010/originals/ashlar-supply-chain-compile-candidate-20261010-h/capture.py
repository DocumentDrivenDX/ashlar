"""Candidate compiler observations only; no expected acceptance or native run."""
import hashlib,json,sys
from pathlib import Path
from ashlar.weft_paths_keys_distribution import PathsKeysDistributionPaths,compile_paths_keys_distribution
ROOT=Path(__file__).resolve().parent
command=json.loads((ROOT/'command.json').read_bytes())
def verify():
 if str(Path(command['interpreter_resolution']['argv_path']).resolve()) != command['interpreter_resolution']['resolved_path']:raise ValueError('candidate-runtime-drift')
 for entry in command['resources']:
  raw=Path(entry['path']).read_bytes()
  if len(raw)!=entry['bytes'] or hashlib.sha256(raw).hexdigest()!=entry['sha256']:raise ValueError('candidate-resource-drift')
verify()
primary=None
try:
 output=Path(command['output']);output.mkdir()
 paths=PathsKeysDistributionPaths(Path(command['index']),Path(command['installation']))
 for case in command['cases']:
  request=(ROOT/(case+'.request.json')).read_bytes()
  response=compile_paths_keys_distribution(paths,request)
  stream=(output/(case+'.response.json')).open('xb')
  failure=None
  try:
   written=stream.write(response)
   if type(written) is not int or written != len(response):raise ValueError('candidate-short-write')
  except BaseException as exc:failure=exc;raise
  finally:
   try:stream.close()
   except BaseException:
    if failure is None:raise
    try:failure.cleanup_failed=True
    except BaseException:pass
except BaseException as exc:
 primary=exc;raise
finally:
 try:verify()
 except BaseException:
  if primary is None:raise
  try:primary.cleanup_failed=True
  except BaseException:pass
