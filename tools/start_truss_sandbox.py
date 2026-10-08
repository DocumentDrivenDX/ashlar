"""Start only the isolated integration PostgreSQL substrate; no Truss claim."""
import json
import os
import secrets
import subprocess

NAME = 'ashlar-e2e-truss-pg17'

def main():
    existing = subprocess.run(['docker', 'container', 'inspect', NAME], capture_output=True, text=True)
    if existing.returncode == 0:
        container = json.loads(existing.stdout)[0]
        if container['Config']['Labels'].get('ashlar.purpose') != 'end-to-end-development':
            raise SystemExit('Refusing unrelated container')
        if container['Config']['Image'] != 'postgres:17.9':
            raise SystemExit('Refusing unexpected PostgreSQL image')
        if not container['State']['Running']:
            subprocess.run(['docker', 'start', NAME], check=True, capture_output=True)
        print(json.dumps({'container': NAME, 'state': 'existing isolated substrate started', 'truss_runtime_installed': False}))
        return
    env = dict(os.environ, POSTGRES_PASSWORD=secrets.token_urlsafe(32))
    subprocess.run(['docker', 'run', '-d', '--name', NAME,
                    '--label', 'ashlar.purpose=end-to-end-development',
                    '--memory', '512m', '--cpus', '1',
                    '-p', '127.0.0.1:15432:5432',
                    '-e', 'POSTGRES_PASSWORD', '-e', 'POSTGRES_DB=truss_e2e',
                    '--health-cmd', 'pg_isready -U postgres -d truss_e2e',
                    '--health-interval', '2s', '--health-timeout', '2s',
                    '--health-retries', '15', 'postgres:17.9'],
                   env=env, check=True, capture_output=True, text=True)
    print(json.dumps({'container': NAME, 'image': 'postgres:17.9',
                      'port': '127.0.0.1:15432', 'memory_limit_mib': 512,
                      'cpu_limit': 1, 'truss_runtime_installed': False}))

if __name__ == '__main__':
    main()
