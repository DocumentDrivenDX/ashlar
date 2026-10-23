"""Static-only checks for the unexecuted attempt-04 JFR preparation."""
import ast
from pathlib import Path
from xml.etree import ElementTree

HERE=Path(__file__).resolve().parent
base=(HERE/'run.py').read_text();candidate=(HERE/'run_jfr.py').read_text()
for name in ('run_jfr.py','supervise_jfr.py'):
 ast.parse((HERE/name).read_text())

def sql_block(text):
 start=text.index('        sql=f"""')
 end=text.index('        explain=',start)
 return text[start:end]
assert sql_block(base)==sql_block(candidate),'JFR runner changed the SQL block'
assert 'spark.sql(sql)' in candidate and candidate.index("JFR.start") < candidate.index('spark.sql(sql)') < candidate.index("JFR.stop")
assert "(out/'insert.sql').write_text(sql)" in candidate,'exact executed SQL bytes must be retained without a display newline'
assert "JFR.configure" in candidate and 'repositorypath={repository}' in candidate and 'configure_readback' in candidate
sup=(HERE/'supervise_jfr.py').read_text()
assert '/private/tmp/ashlar-edge-write-attribution-20261023-attempt-04' in sup
assert "str(EVIDENCE/'run_jfr.py')" in sup
assert '4_000_000_000' in sup and '128_000_000' in sup and 'elapsed>=290' in sup and 'min(8,remaining)' in sup
root=Path('/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home')
profile=root/'lib/jfr/profile.jfc'
events={event.attrib['name']:{setting.attrib['name']:setting.text for setting in event.findall('setting')} for event in ElementTree.parse(profile).getroot().findall('event')}
assert events['jdk.ExecutionSample']['period']=='10 ms'
assert events['jdk.ObjectAllocationSample']['throttle']=='300/s'
assert 'JFR.start' in candidate and "'settings=profile'" in candidate and "'maxsize=32M'" in candidate and "'disk=true'" in candidate
assert not Path('/private/tmp/ashlar-edge-write-attribution-20261023-attempt-04').exists()
print('attempt-04 JFR preparation is static-only; SQL/runtime unchanged, profile and monitored bounds verified; no JVM started')
