/** Validate and copy an already decoded publication locator for one read plan.
 * Descriptor decoding, duplicate JSON-member refusal, authority and semantic
 * projection validation belong to the caller; this is not publication proof.
 */
import type { Publication } from './native-read';
function tableName(value: string): boolean {
  return typeof value === 'string' && /^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$/.test(value);
}
export function publicationLocator(input: Publication, requiredTables: readonly string[]): Publication {
  if (!input || !['ashlar-delta/0.2','ashlar-delta/0.3'].includes(input.profile)) throw new Error('Unsupported publication profile');
  if (typeof input.publicationId !== 'string' || !input.publicationId) throw new Error('Missing publication identity');
  if (!Array.isArray(requiredTables) || !requiredTables.length || new Set(requiredTables).size !== requiredTables.length || requiredTables.some(t => !tableName(t))) throw new Error('Invalid read-plan table inventory');
  const vector=input.tableVersions;
  if (!vector || typeof vector !== 'object' || Array.isArray(vector)) throw new Error('Invalid publication vector');
  const versions: Record<string,number> = Object.create(null);
  for (const [table,version] of Object.entries(vector)) {
    if (!tableName(table) || !Number.isSafeInteger(version) || version < 0) throw new Error('Malformed publication vector entry');
    versions[table]=version;
  }
  for (const table of requiredTables) {
    if (!Object.prototype.hasOwnProperty.call(versions,table)) throw new Error('Read-plan table absent from publication vector');
  }
  return Object.freeze({profile:input.profile, publicationId:input.publicationId, tableVersions:Object.freeze(versions)});
}
