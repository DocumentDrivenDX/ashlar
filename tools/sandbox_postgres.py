"""Private isolated development connection factory; no credentials are emitted."""
import json
import subprocess
import psycopg


def connect(role):
    if role not in {'ashlar_outbox_reader', 'ashlar_outbox_writer',
                    'ashlar_pin_reader', 'ashlar_pin_writer', 'ashlar_pin_maintenance',
                    'ashlar_pin_recovery'}:
        raise PermissionError('Explicit ordinary sandbox role required')
    container = json.loads(subprocess.check_output(
        ['/usr/local/bin/docker', 'inspect', 'ashlar-e2e-truss-pg17']))[0]
    if container['Config']['Labels'].get('ashlar.purpose') != 'end-to-end-development':
        raise ValueError('Wrong isolated development container')
    password = next(value.split('=', 1)[1] for value in container['Config']['Env']
                    if value.startswith('POSTGRES_PASSWORD='))
    return psycopg.connect(host='127.0.0.1', port=15432, user='postgres',
        password=password, dbname='truss_e2e', connect_timeout=5,
        options='-c role=' + role + ' -c statement_timeout=5000 -c lock_timeout=1000')
