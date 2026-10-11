export interface SourceArtifact {
    id: string;
    reference: string;
    url: string;
    format: string;
    dataKind: string;
    sha256: string;
    revision?: string;
}
export interface Entry {
    id: string;
    title: string;
    category: string;
    path: string;
    text?: string;
    url?: string;
    revisions?: Revision[];
    annotations?: Record<string, Annotation[]>;
    format: 'json' | 'yaml';
    pack?: string;
    description?: string;
    schemaFormat?: string;
    packVersion?: string;
    aliases?: string[];
    assets?: SourceArtifact[];
}
/** Catalog sources are inspected as data, never executed. */
export interface SchemaBrowserOptions {
    /** URL of the hosted package's assets/ directory; must be HTTP(S). */
    assetsUrl: string;
    /** Caller-supplied catalog. Omit to load catalogUrl instead. */
    entries?: Entry[];
    catalog?: Catalog;
    categories?: Category[];
    selection?: Selection;
    annotations?: Record<string, Annotation[]>;
    readOnlyLocalFiles?: boolean;
    theme?: 'light' | 'dark' | 'auto';
    themeVariables?: Record<string, string>;
    hideHero?: boolean;
    onContentHeight?: (height: number) => void;
    onSelect?: (selection: Selection) => void;
    onNavigate?: (selection: Selection) => void;
    onReady?: () => void;
    onCatalogError?: (error: BrowserError) => void;
    onError?: (error: BrowserError) => void;
    catalogUrl?: string;
    /** Base URL for declared relative original/download references. */
    assetBaseUrl?: string;
    /** URLSearchParams spelling, for example schema=...&definition=... */
    initialRoute?: string;
    title?: string;
    height?: string;
}
export interface SchemaBrowserHandle {
    iframe: HTMLIFrameElement;
    select(selection: Selection): Promise<Selection>;
    focus(): void;
    setAnnotations(annotations: Record<string, Annotation[]>): void;
    /** Catalog and module initialized; rejects on failure or a 30-second initialization timeout. */
    ready: Promise<void>;
    destroy(): void;
}
export interface Category {
    id: string;
    label: string;
    order?: number;
}
export type Tone = 'neutral' | 'info' | 'positive' | 'warning' | 'negative';
export interface Annotation {
    badge: string;
    tone?: Tone;
    tooltip?: string;
}
export interface Selection {
    schema: string;
    definition?: string;
    relationship?: string;
    revision?: string;
}
export interface BrowserError {
    code: string;
    message: string;
    status?: number;
    url?: string;
    phase?: 'catalog' | 'selection';
    requestId?: string;
}
export interface Catalog {
    version: 1;
    entries: Entry[];
    categories?: Category[];
}
export interface Revision {
    id: string;
    label?: string;
    text?: string;
    url?: string;
    format: 'json' | 'yaml';
}
