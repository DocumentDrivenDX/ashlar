import type { SchemaBrowserOptions, SchemaBrowserHandle } from './types.js';
export type * from './types.js';
export { renderPropertyTable, renderRecordMap, compareDocuments, renderDocumentDiff, comparisonModel } from './components.js';
export declare function browserDocument(options: SchemaBrowserOptions, baseUrl: string, instance?: string): string;
export declare function mountSchemaBrowser(container: HTMLElement, options: SchemaBrowserOptions): SchemaBrowserHandle;
export declare const mount: typeof mountSchemaBrowser;
export type { ComponentModel, ChangeStatus, DocumentChange } from './components.js';
