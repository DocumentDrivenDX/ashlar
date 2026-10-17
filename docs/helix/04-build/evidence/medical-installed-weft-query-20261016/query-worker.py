from pathlib import Path
import argparse,hashlib,json,os,sys
from contextlib import contextmanager,redirect_stdout
from ashlar_host.lifecycle import finish
from ashlar_host.original_medical_local_configuration import parse_original_medical_local
from ashlar_host.medical_process_supervision import OriginalMedicalNativePhase
from ashlar_host.medical_query_local import query_original_medical_cases
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths
from ashlar_host.medical_failure_location import medical_failure_location
@contextmanager
def diagnostic_interval():
 descriptor=None;primary=None
 try:
  sys.stdout.flush();sys.stderr.flush();descriptor=os.dup(1);os.dup2(2,1)
  with redirect_stdout(sys.stderr):yield
 except BaseException as error:primary=error
 finish(primary,[sys.stderr.flush]+([]if descriptor is None else[lambda:os.dup2(descriptor,1),lambda:os.close(descriptor)]))
p=argparse.ArgumentParser();p.add_argument('--native-phase-fd',type=int,required=True);args=p.parse_args()
config_path=Path('/private/tmp/ashlar-medical-query-installed-20261016-v/read-query-configuration.json')
raw=config_path.read_bytes()
if hashlib.sha256(raw).hexdigest()!='407eaabbbd80b7864948217522a2d599752485f5ea0b7e7a157a7bf5af610635':raise PermissionError('Original fixed read-query selection differs')
configuration=parse_original_medical_local(raw)
paths=ImplicitArithmeticDistributionPaths(Path('/private/tmp/ashlar-payments-held-guard-proposal-20261016-upgg9_5l/original-index.json'),Path('/private/tmp/ashlar-payments-held-guard-proposal-20261016-upgg9_5l/fresh-installation'))
try:
 with diagnostic_interval():
  answer=query_original_medical_cases(configuration,paths,supervision=OriginalMedicalNativePhase(args.native_phase_fd))
 if config_path.read_bytes()!=raw:raise PermissionError('Closing fixed read-query selection differs')
 result=(json.dumps(answer,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
 if len(result)>configuration.bounds.receipt_bytes:raise ValueError('Bounded closed SDK result required')
 sys.stdout.buffer.write(result);sys.stdout.buffer.flush()
except BaseException as error:
 try:
  payload=medical_failure_location(error);fd=os.open(config_path.parent/'worker-failure-location.json',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
  with os.fdopen(fd,'wb')as out:out.write(payload);out.flush();os.fsync(out.fileno())
 except BaseException:pass
 sys.stderr.write('original medical read/query refused\n');sys.stderr.flush()
 raise SystemExit(2)from None
