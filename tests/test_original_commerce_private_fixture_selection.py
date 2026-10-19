# SPDX-License-Identifier: MIT OR Apache-2.0
"""Proposed fixture config uses existing bootstrap owner; no PG/Docker calls."""
import copy,json,unittest
from pathlib import Path
from setup_private_outbox_fixture import Source,Settings,ddl
from ashlar_host.ack import AckScope

EXAMPLE=Path(__file__).resolve().parents[1]/'examples/private-outbox-original-commerce/configuration.example.json'

def selected(value):
    sources=tuple(Source(AckScope(**row['scope']),row['schema'],row['writer'],row['reader'],row['ack'])for row in value['sources'])
    return Settings(Path(value['docker']),value['docker_host'],value['image'],value['container'],value['port'],value['database'],value['authentication'],sources,Path(value['output']))

class Tests(unittest.TestCase):
    def test_exact_original_scope_and_isolated_ordinary_ddl(self):
        value=json.loads(EXAMPLE.read_bytes());config=selected(value)
        first,unused=config.sources
        self.assertEqual((first.scope.feed,first.scope.epoch),('private-original-commerce-fixture','original-commerce-v1'))
        self.assertNotEqual(first.scope.installation_id,unused.scope.installation_id)
        self.assertNotEqual(first.scope.feed,unused.scope.feed)
        self.assertEqual(config.docker_host,'unix:///Users/erik/.orbstack/run/docker.sock')
        for source in config.sources:
            sql='\n'.join(ddl(source))
            for role in (source.writer,source.reader,source.ack):
                self.assertIn('ALTER ROLE '+role+' LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;',sql)
            self.assertIn(source.schema,sql);self.assertIn(source.scope.service_schema,sql)
            self.assertNotIn('GRANT ALL',sql)
    def test_foreign_remote_or_shared_scope_refuses_before_effects(self):
        original=json.loads(EXAMPLE.read_bytes())
        for change in ('remote','alias-role','shared-installation','port-bool','different-image'):
            value=copy.deepcopy(original)
            if change=='remote':value['docker_host']='tcp://remote:2375'
            elif change=='alias-role':value['sources'][0]['ack']=value['sources'][0]['reader']
            elif change=='shared-installation':value['sources'][1]['scope']['installation_id']=value['sources'][0]['scope']['installation_id']
            elif change=='port-bool':value['port']=True
            else:value['image']='postgres:latest'
            with self.subTest(change=change),self.assertRaises(ValueError):selected(value)

if __name__=='__main__':unittest.main()
