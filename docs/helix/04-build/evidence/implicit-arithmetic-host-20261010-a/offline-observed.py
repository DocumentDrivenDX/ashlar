import copy
from test_implicit_arithmetic import PAIRS,SCHEMAS
from ashlar_host.count_star_schema import make_offline_implicit_arithmetic_schema_validation
from ashlar_host.implicit_arithmetic_profile import ImplicitArithmeticAdmissionConfig
from ashlar_host.count_star_admission import admit_implicit_arithmetic_artifact,CountStarPlanError
port=make_offline_implicit_arithmetic_schema_validation(SCHEMAS);cfg=ImplicitArithmeticAdmissionConfig(16777216,port)
for name,(q,r) in PAIRS.items():
 admit_implicit_arithmetic_artifact(q,r,copy.deepcopy(r),config=cfg)
 bad=copy.deepcopy(r);bad['logicalPlan']['outputs'][0]['expression']['op']='invented'
 try:admit_implicit_arithmetic_artifact(q,bad,copy.deepcopy(bad),config=cfg)
 except CountStarPlanError:pass
 else:raise AssertionError(name)
print('Actual offline Draft2020-12: four original042 pairs accepted, four malformed original-expression controls refused')
