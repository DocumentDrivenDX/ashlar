import { UmfError, type Json, pointer } from './types';
export const LIMITS = { maxDepth: 128, maxValues: 100_000, maxTextLength: 4_000_000 } as const;
/** Copy JSON data without invoking getters, toJSON, or custom prototypes. */
export function copyJson(input: unknown): Json {
  const active = new Set<object>();
  let values = 0;
  function visit(value: unknown, path: string, depth: number): Json {
    if (++values > LIMITS.maxValues || depth > LIMITS.maxDepth) throw new UmfError('LIMIT', 'JSON structural limit exceeded', path);
    if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
    if (typeof value === 'number') {
      if (!Number.isFinite(value) || Object.is(value, -0) || (Number.isInteger(value) && !Number.isSafeInteger(value))) throw new UmfError('NUMBER', 'Number outside the core interoperable numeric profile', path);
      return value;
    }
    if (typeof value !== 'object') throw new UmfError('NON_JSON', 'Value is not JSON-compatible', path);
    if (active.has(value)) throw new UmfError('CYCLE', 'Cyclic value', path);
    const proto = Object.getPrototypeOf(value);
    if (Array.isArray(value) ? proto !== Array.prototype : proto !== Object.prototype && proto !== null) throw new UmfError('NON_JSON', 'Custom object prototype', path);
    if (Object.getOwnPropertySymbols(value).length) throw new UmfError('NON_JSON', 'Symbol keys are not JSON-compatible', path);
    active.add(value);
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const result: Json[] | Record<string, Json> = Array.isArray(value) ? [] : Object.create(null);
    for (const [key, descriptor] of Object.entries(descriptors)) {
      if (Array.isArray(value) && key === 'length') continue;
      if (!descriptor.enumerable || !('value' in descriptor)) throw new UmfError('NON_JSON', 'Hidden fields and accessors cannot be serialized faithfully', path + '/' + pointer(key));
      if (Array.isArray(value) && !/^(0|[1-9][0-9]*)$/.test(key)) throw new UmfError('NON_JSON', 'Non-index array property', path);
      const item = visit(descriptor.value, path + '/' + pointer(key), depth + 1);
      Object.defineProperty(result, key, { value: item, enumerable: true, writable: true, configurable: true });
    }
    if (Array.isArray(value) && Object.keys(value).length !== value.length) throw new UmfError('NON_JSON', 'Sparse arrays are not JSON values', path);
    active.delete(value);
    return result;
  }
  return visit(input, '', 0);
}
