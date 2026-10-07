"""Read-only persistent-connection SELECT1 latency control."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_client_floor_r143'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
c=DriverClient(O);assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
for i in range(30):assert c.sql('constant-'+str(i),'SELECT 1',tag=False)==[['1']]
c.history();c.close();print('Thirty persistent SELECT1 controls passed; final metrics pending')
