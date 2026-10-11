import type { Annotation } from './types.js';
export type ChangeStatus = 'created' | 'changed' | 'removed' | 'unchanged';
export interface ComponentModel {
    records: {
        id: string;
        label?: string;
        properties?: number;
        status?: ChangeStatus;
    }[];
    edges: {
        id: string;
        source: string;
        target: string;
        label?: string;
        directed?: boolean;
        status?: ChangeStatus;
    }[];
}
export declare function renderPropertyTable(root: HTMLElement, properties: Record<string, unknown>, options?: {
    annotations?: Record<string, Annotation[]>;
}): {
    destroy(): void;
};
export declare function renderRecordMap(root: HTMLElement, model: ComponentModel, options?: {
    focus?: string;
    annotations?: Record<string, Annotation[]>;
    onNavigate?: (id: string, kind?: 'record' | 'relationship') => void;
    onFocusChange?: (id: string) => void;
}): {
    update(next: ComponentModel): void;
    destroy(): void;
};
export interface DocumentChange {
    path: string;
    status: Exclude<ChangeStatus, 'unchanged'>;
    before?: unknown;
    after?: unknown;
}
/** Structural comparison: original sources are never rewritten. Numeric strings remain exact. */
export declare function compareDocuments(before: unknown, after: unknown): DocumentChange[];
export declare function renderDocumentDiff(root: HTMLElement, before: unknown, after: unknown): {
    changes: DocumentChange[];
    destroy(): void;
};
export declare function comparisonModel(before: any, after: any): ComponentModel;
