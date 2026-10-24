// SPDX-License-Identifier: MIT OR Apache-2.0
import {test, expect} from 'bun:test';
import {datasetInput, CarrierRefusal, writeFreshReceipt, BOOLEAN_LEXICAL_PROFILE, publicBooleanLiteral, parseDatasetCliOptions} from '../tools/check_domain_pack_datasets';
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

test('focused dataset selection remains explicit and rejects malformed or duplicate scope', () => {
  expect(parseDatasetCliOptions(['producer', 'packs', 'out', '--pack', 'supply-chain',
    '--boolean-lexical-profile', BOOLEAN_LEXICAL_PROFILE])).toEqual({producer: 'producer', packs: 'packs', output: 'out',
    booleanSelected: true, selectedPack: 'supply-chain'});
  expect(() => parseDatasetCliOptions(['producer', 'packs', 'out', '--pack', 'supply-chain', '--pack', 'commerce'])).toThrow();
  expect(() => parseDatasetCliOptions(['producer', 'packs', 'out', '--pack', '../commerce'])).toThrow();
  expect(() => parseDatasetCliOptions(['producer', 'packs', 'out', '--boolean-lexical-profile', 'latest'])).toThrow();
});

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


test('explicit public Boolean receipt preserves exact lexical context, omitted and null carriers', () => {
  const {source, graph} = fixture(); source.modules[0].elements[1].scalarType = 'boolean';
  graph.objects[2].values.note = 'True';
  const original = JSON.stringify({source, graph}); const calls: any[] = []; const receipts: any[] = [];
  const admission = {profile: BOOLEAN_LEXICAL_PROFILE as typeof BOOLEAN_LEXICAL_PROFILE, sourceContext: {graphSha256: 'original-graph'}, receipts,
    validateCsvBooleanLexical(s: any, request: any) {
      calls.push(['validate', structuredClone(s), structuredClone(request)]);
      // This injected public-port response is not a semantic authority proof.
      s.id = 'callback-mutated'; request.token = 'mutated';
      return {value: {boolean: true}, validation: {valid: true, complete: true}, request: calls[0][2]};
    },
    verifyCsvBooleanLexical(receipt: any, s: any, request: any) {
      calls.push(['verify', structuredClone(s), structuredClone(request)]); return receipt;
    }};
  expect(() => datasetInput(source, graph, 'scope')).toThrow(CarrierRefusal);
  const result = datasetInput(source, graph, 'scope', admission);
  expect(result.records.map(r => r.values[1])).toEqual([
    {field: {module: 'm', element: 'note'}, state: 'absent'},
    {field: {module: 'm', element: 'note'}, state: 'present', value: null},
    {field: {module: 'm', element: 'note'}, state: 'present', value: {boolean: true}},
  ]);
  expect(calls.map(c => c[0])).toEqual(['validate', 'verify']);
  expect(calls[1][1]).toEqual(source); expect(calls[1][2]).toEqual(calls[0][2]);
  expect(calls[1][2].sourceContext).toEqual({graphSha256: 'original-graph', objectKey: 'c', fieldIdentity: {module: 'm', element: 'note'}});
  expect(calls[1][2].token).toBe('True'); expect(receipts).toHaveLength(1);
  expect(JSON.stringify({source, graph})).toBe(original);
});

test('malformed Boolean token public refusal and incomplete verifier results are never admitted', () => {
  const {source, graph} = fixture(); source.modules[0].elements[1].scalarType = 'boolean'; graph.objects[2].values.note = 'TRUE';
  const admission = {profile: BOOLEAN_LEXICAL_PROFILE as typeof BOOLEAN_LEXICAL_PROFILE, sourceContext: {}, receipts: [] as any[],
    validateCsvBooleanLexical() {throw new CarrierRefusal('Public lexical refusal');},
    verifyCsvBooleanLexical() {throw Error('Not reached');}};
  expect(() => datasetInput(source, graph, 'scope', admission)).toThrow('Public lexical refusal'); expect(admission.receipts).toHaveLength(0);
  for (const verified of [false, {}, {validation: {valid: false, complete: true}, value: {boolean: true}}, {validation: {valid: true, complete: false}, value: {boolean: true}}]) {
    const port = {...admission, validateCsvBooleanLexical: () => ({}), verifyCsvBooleanLexical: () => verified};
    expect(() => publicBooleanLiteral(source, {module: 'm', element: 'note'}, 'True', {}, port)).toThrow(CarrierRefusal);
    expect(port.receipts).toHaveLength(0);
  }
});
