/** Portable spike query builder; caller supplies authenticated SQL execution. */
export type Publication = {
  profile: 'ashlar-delta/0.2' | 'ashlar-delta/0.3';
  publicationId: string;
  tableVersions: Readonly<Record<string, number>>;
};
export type Identity = { sourceSystem: string; typeId: string; id: string };
export type Query = { sql: string; parameters: Readonly<Record<string, string>> };

function decimal64(value: string): string {
  if (typeof value !== 'string') throw new Error('Identity must be exact decimal text');
  if (!/^(0|-?[1-9][0-9]*)$/.test(value)) throw new Error('Noncanonical integer identity');
  const n = BigInt(value);
  if (n < -(1n << 63n) || n >= (1n << 63n)) throw new Error('Identity outside signed int64 profile');
  return value;
}
function pinnedTable(publication: Publication, table: string): string {
  if (!['ashlar-delta/0.2', 'ashlar-delta/0.3'].includes(publication.profile)) throw new Error('Unsupported table profile');
  if (typeof publication.publicationId !== 'string' || !publication.publicationId) throw new Error('Missing publication identity');
  const parts = table.split('.');
  if (parts.length !== 3 || parts.some(p => !/^[A-Za-z_][A-Za-z0-9_]*$/.test(p))) throw new Error('Invalid table identifier');
  if (!Object.prototype.hasOwnProperty.call(publication.tableVersions, table)) throw new Error('Table absent from publication vector');
  const version = publication.tableVersions[table];
  if (!Number.isSafeInteger(version) || version < 0) throw new Error('Invalid pinned Delta version');
  return parts.map(p => '`' + p + '`').join('.') + ' VERSION AS OF ' + version;
}
export function singletonQuery(publication: Publication, table: string, kind: 'node' | 'edge', identity: Identity): Query {
  if (kind !== 'node' && kind !== 'edge') throw new Error('Unsupported entity kind');
  if (typeof identity.sourceSystem !== 'string' || !/^[\x00-\x7f]+$/.test(identity.sourceSystem)) throw new Error('Source outside ASCII identity profile');
  const typeField = kind === 'node' ? 'type_id' : 'rel_type_id';
  const expectedTable = kind === 'node' ? 'object_current' : 'edge_current';
  if (table.split('.').at(-1) !== expectedTable) throw new Error('Entity kind/table mismatch');
  const parameters = { source: identity.sourceSystem, type: decimal64(identity.typeId), id: decimal64(identity.id) };
  const hash = `sha2(to_json(named_struct('source_system', :source, '${typeField}', CAST(:type AS BIGINT), 'id', CAST(:id AS BIGINT))), 256)`;
  return { sql: `SELECT * FROM ${pinnedTable(publication, table)} WHERE lookup_hash = ${hash} AND source_system = :source AND ${typeField} = CAST(:type AS BIGINT) AND id = CAST(:id AS BIGINT)`, parameters };
}
// Decode *_json with an exact parser only; this builder returns no parsed values.
// A publication object is a locator, not authorization or proof of native custody.

export type AdjacencyCursor = { relTypeId: string; edgeId: string };
export function adjacencyQuery(publication: Publication, table: string, direction: 'out' | 'in', identity: Identity, pageSize: number, after?: AdjacencyCursor): Query {
  if (direction !== 'out' && direction !== 'in') throw new Error('Unsupported direction');
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 1000) throw new Error('Page size outside bounded profile');
  if (typeof identity.sourceSystem !== 'string' || !/^[\x00-\x7f]+$/.test(identity.sourceSystem)) throw new Error('Source outside ASCII identity profile');
  const expected = direction === 'out' ? 'adjacency_forward' : 'adjacency_reverse';
  if (table.split('.').at(-1) !== expected) throw new Error('Direction/table mismatch');
  const prefix = direction === 'out' ? 'source' : 'target';
  const parameters: Record<string,string> = { source: identity.sourceSystem, type: decimal64(identity.typeId), id: decimal64(identity.id) };
  let continuation = '';
  if (after) {
    parameters.afterType = decimal64(after.relTypeId);
    parameters.afterEdge = decimal64(after.edgeId);
    continuation = ' AND (rel_type_id > CAST(:afterType AS BIGINT) OR (rel_type_id = CAST(:afterType AS BIGINT) AND edge_id > CAST(:afterEdge AS BIGINT)))';
  }
  return { sql: `SELECT * FROM ${pinnedTable(publication,table)} WHERE source_system = :source AND ${prefix}_type = CAST(:type AS BIGINT) AND ${prefix}_id = CAST(:id AS BIGINT)${continuation} ORDER BY rel_type_id, edge_id LIMIT ${pageSize}`, parameters };
}
// Resume only under the same publication/table/direction/endpoint context.
// The caller must bind the cursor to that context; this is a SQL builder only.
