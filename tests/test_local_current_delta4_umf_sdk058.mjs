// Effect-free source-backed IIFE comparison against the retained SDK 0.5.8 packet.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const resource = new URL('../src/ashlar_host/resources/local-current-delta4-sdk058/', import.meta.url);
const packet = new URL('../docs/helix/04-build/evidence/current-upstream-release-20261029/g03-sdk058-delta4-realization-candidate-20261030/', import.meta.url);
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const bundle = readFileSync(new URL('umf-spark-delta4.iife.js', resource));
const proposalBytes = readFileSync(new URL('outputs/carrier-proposals.json', packet));
assert.equal(hash(bundle), '8ca2c7ec908c80b78d4bac66144511330f56ad722b9149175c6d5fb0495bcb93');
assert.equal(hash(proposalBytes), 'd00c4a9d0c6c1fd0946007a5ae2495ee95960ba094d1a46dbed0dc6d966e68ea');
const retained = JSON.parse(proposalBytes);
vm.runInThisContext(bundle.toString('utf8'));
const bridge = globalThis.__ashlarUmfSparkDelta4Sdk058;
assert.ok(bridge && typeof bridge.realize === 'function');

for (const item of retained.roles) {
  const source = readFileSync(new URL(`inputs/carriers/${item.role}.umf.json`, packet));
  assert.equal(source.byteLength, item.sourceBytes, `${item.role} source length`);
  assert.equal(hash(source), item.sourceSha256, `${item.role} source identity`);
  const target = {kind: 'delta-path', path: `/virtual/ashlar-g03-umf058/${item.role}`};
  const actual = bridge.realize(source.toString('utf8'), target);
  assert.deepEqual(JSON.parse(JSON.stringify(actual)), item.release, `${item.role} proposal`);
}

assert.throws(() => bridge.realize('{', {
  kind: 'delta-path', path: '/virtual/ashlar-g03-umf058/object_current',
}));
