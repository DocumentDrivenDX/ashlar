import {test, expect} from 'bun:test';
import {datasetInput, CarrierRefusal, writeFreshReceipt} from '../tools/check_domain_pack_datasets';
import {mkdtemp, readFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

function fixture() {
  const id = {module: 'm', element: 'id'}, note = {module: 'm', element: 'note'};
  const source = {umf: '0.8.0', id: 'domain', modules: [{id: 'm', elements: [
    {id: 'id', scalarType: 'string'}, {id: 'note', scalarType: 'string'},
    {id: 'row', members: [id, note], keys: [{id: 'pk', fields: [id]}]},
  ], relationships: [{id: 'link', target: [{module: 'm', element: 'row', key: 'pk'}]}]}]};
  const row = (key: string, values: any) => ({key, type: {document: 'domain', module: 'm', element: 'row'}, values});
  const graph = {format: 'umf.domain-graph', version: '1.0.0', qualification: 'test',
    objects: [row('a', {id: '01'}), row('b', {id: '1', note: null}), row('c', {id: 'é', note: ''})],
    edges: ['first', 'parallel'].map(key => ({key, relationship: {document: 'domain', module: 'm', id: 'link'}, source: 'a', target: 'b'}))};
  return {source, graph};
}

test('preserves omitted/null/empty, opaque keys and independent parallel occurrences without mutation', () => {
  const {source, graph} = fixture(); const before = JSON.stringify({source, graph});
  const input = datasetInput(source, graph, 'scope');
  expect(input.records.map(r => r.values[1])).toEqual([
    {field: {module: 'm', element: 'note'}, state: 'absent'},
    {field: {module: 'm', element: 'note'}, state: 'present', value: null},
    {field: {module: 'm', element: 'note'}, state: 'present', value: {string: ''}},
  ]);
  expect(input.records.map(r => r.values[0].value)).toEqual([{string: '01'}, {string: '1'}, {string: 'é'}]);
  expect(input.relationships.map(r => [r.instanceId, r.target.values])).toEqual([
    ['first', [{string: '1'}]], ['parallel', [{string: '1'}]],
  ]);
  expect(JSON.stringify({source, graph})).toBe(before);
});

test('exact numeric tokens are not normalized or narrowed before public verification', () => {
  const {source, graph} = fixture(); source.modules[0].elements[0].scalarType = 'integer';
  graph.objects[0].values.id = '9007199254740993';
  graph.objects[1].values.id = '01';
  expect(datasetInput(source, graph, 'scope').records[0].values[0].value).toEqual({integerToken: '9007199254740993'});
  expect(datasetInput(source, graph, 'scope').relationships[0].target.values).toEqual([{integerToken: '01'}]);
});

test('unmapped, ambiguous and unsupported carriers refuse rather than disappearing', () => {
  for (const variant of ['extra', 'field-name', 'float', 'missing-key', 'missing-target', 'wrong-document']) {
    const {source, graph} = fixture();
    if (variant === 'extra') graph.objects[0].values.extra = 'retained';
    if (variant === 'field-name') source.modules[0].elements[2].members!.push({module: 'other', element: 'id'});
    if (variant === 'float') source.modules[0].elements[0].scalarType = 'float';
    if (variant === 'missing-key') delete graph.objects[1].values.id;
    if (variant === 'missing-target') graph.edges[0].target = 'missing';
    if (variant === 'wrong-document') graph.objects[0].type.document = 'other';
    expect(() => datasetInput(source, graph, 'scope')).toThrow(CarrierRefusal);
  }
});

test('a late existing output cannot replace original receipt bytes', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'ashlar-pack-receipt-'));
  const output = join(directory, 'receipt.json');
  try {
    await writeFreshReceipt(output, 'original\n');
    await expect(writeFreshReceipt(output, 'replacement\n')).rejects.toThrow();
    expect(await readFile(output, 'utf8')).toBe('original\n');
  } finally { await rm(directory, {recursive: true}); }
});
