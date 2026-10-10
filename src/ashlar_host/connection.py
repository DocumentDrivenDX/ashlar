"""Selected local host implementation; native qualification remains version-scoped."""
import json, re, subprocess
CONTAINER = 'ashlar-e2e-truss-pg17'

def connect(role):
    if type(role) is not str or len(role) > 63 or (not re.fullmatch('ashlar_ack_[a-z0-9_]+', role)):
        raise PermissionError('Explicit private ACK fixture role required')
    result = json.loads(subprocess.check_output(['docker', 'inspect', CONTAINER]))
    if len(result) != 1:
        raise ValueError('Single original private container required')
    container = result[0]
    if container['Config']['Labels'].get('ashlar.purpose') != 'end-to-end-development':
        raise ValueError('Wrong private development container')
    if container['NetworkSettings']['Ports'].get('5432/tcp') != [{'HostIp': '127.0.0.1', 'HostPort': '15432'}]:
        raise ValueError('Exact loopback private endpoint required')
    values = [v.split('=', 1)[1] for v in container['Config']['Env'] if v.startswith('POSTGRES_PASSWORD=')]
    if len(values) != 1 or not values[0]:
        raise ValueError('Private container authentication unavailable')
    import psycopg
    connection = psycopg.connect(host='127.0.0.1', port=15432, user='postgres', password=values[0], dbname='truss_e2e', connect_timeout=5, options='-c statement_timeout=5000 -c lock_timeout=1000')
    try:
        connection.execute('SET SESSION AUTHORIZATION ' + role)
        observed = connection.execute('SELECT session_user,current_user').fetchone()
        if observed != (role, role):
            raise PermissionError('Original ordinary session identity differs')
        connection.commit()
        if connection.autocommit or connection.info.transaction_status != 0:
            raise ValueError('Fresh independent non-autocommit connection required')
        return connection
    except BaseException as primary:
        try:
            connection.close()
        except BaseException:
            try:
                primary.cleanup_failed = True
            except BaseException:
                pass
        raise
