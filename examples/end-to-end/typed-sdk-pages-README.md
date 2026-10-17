# Typed pages from a configured graph publication

Use the [typed graph installation](typed-graph-README.md) and its original source
configuration. Select a separate Python 3.11 environment with `local-delta`, the
qualified UMF `umf_access` SDK wheel and published `weft-sql==0.4.41` (`typed-sdk`
extra). Keep the original installation and model pins. Prepare a normal
configured-local read invocation with a fresh output directory; its runtime
settings select the Python, JDK, JARs, source and installation explicitly.

The Python page workflow reuses `AshlarTarget` and the original Weft engine-page
program. Standalone strict view models are supported; generated DTOs can also be
supplied when their output signatures match.

```python
from pathlib import Path
import weft
from umf_access.types import StrictModel
from ashlar_host.configured_local_cli import parse_configured_local
from ashlar_host.configured_local_runtime import page_configured_local
from ashlar_host.configured_sdk_pages import ConfiguredSdkPagesConfig
from ashlar_host.path_capture import PathCaptureConfig

class Parameters(StrictModel):
    pass

class PersonSummary(StrictModel):
    person_key: str
    person_name: str

runtime = parse_configured_local(Path('/absolute/read.json').read_bytes()).runtime
query = ConfiguredSdkPagesConfig(
    compiler=weft,
    sql='SELECT p.person_key, p.person_name FROM Person p',
    parameters_type=Parameters,
    result_type=PersonSummary,
    parameters=Parameters(),
    page_size=2,
    maximum_rows=100,
    timeout_seconds=90,
    capture=PathCaptureConfig(1000, 65536, 1048576),
)
answer = page_configured_local(runtime, ordinal=1, query=query)
print(answer['query']['items'])
```

The result retains page boundaries, original programs and independent source
witnesses. `has_more` reports truncation at the total row bound. Each page uses
its original authored primary key, hidden lookahead output and SDK continuation;
returned items exclude hidden keys. Selecting ordinal 1 preserves its rows after
later publications change or delete them. Named required-STRING equality filters
may use explicit strict parameter fields, for example `WHERE p.person_name =
:name` with `name: str` and `Parameters(name='Ada')`.

This Python workflow covers one Record's required, facetless, NUL-free STRING
fields and complete STRING primary key. Its explicit page binding leaves other
homes unselected while retaining the complete original model and validating all
source values. Optional/numeric outputs, joins and graph traversals use separately
qualified query paths. The deadline bounds the remaining page-work budget; the
operator still owns the outer process timeout. All enclosing alias, source and
native runtime closing must succeed before the final result or runtime report
is released. Local lease custody and file progress grant no PostgreSQL pins,
remote writer fencing or protected producer ACK.
