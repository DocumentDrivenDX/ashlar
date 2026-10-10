"""Selected local host implementation; native qualification remains version-scoped."""
import re

def fixture_columns(root):
    columns = {}
    baseline = (root / 'sql/ashlar-delta-v03/01-baseline.sql').read_text()
    for table in ['object_current', 'edge_current', 'tombstone']:
        body = re.search('CREATE TABLE ' + table + ' \\((.*?)\\) USING', baseline, re.S).group(1)
        columns[table] = tuple(((m.group(1), m.group(2)) for part in body.split(',') for m in [re.match('\\s*(\\w+) (STRING|BIGINT|TIMESTAMP)', part)] if m))
    columns['whole_source_history'] = tuple(((k, 'STRING') for k in ['feed', 'epoch', 'delivery_id', 'digest', 'change_json', 'raw_base64']))
    return columns
