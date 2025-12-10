/**
 * TDB+ Native Storage Engine
 *
 * High-performance storage engine using the Rust core via N-API.
 */

import { StorageEngine } from './StorageEngine';
import { StorageOptions, StoredDocument, StorageStats, CollectionName } from '../types';
import { Database, NativeDatabase } from '../native';

export class NativeStorage extends StorageEngine {
    private db: NativeDatabase | null = null;
    private basePath: string;

    constructor(options: StorageOptions) {
        super(options);
        this.basePath = options.path || './tdb_data';
    }

    async initialize(): Promise<void> {
        console.log(`Initializing Native Storage at ${this.basePath}`);
        this.db = await Database.open(this.basePath);
    }

    async close(): Promise<void> {
        if (this.db) {
            await this.db.close();
            this.db = null;
        }
    }

    async listCollections(): Promise<CollectionName[]> {
        // Rust core currently doesn't expose list_collections in the binding yet
        // This is a placeholder or we need to add it to bindings
        // For now returning empty or we rely on filesystem check if strictly needed
        return [];
    }

    async loadCollection(name: CollectionName): Promise<StoredDocument[]> {
        if (!this.db) throw new Error('Database not initialized');

        const col = this.db.collection(name);
        const docsJson = await col.scan();

        return docsJson.map(json => JSON.parse(json));
    }

    async saveDocument(collection: CollectionName, document: StoredDocument): Promise<void> {
        if (!this.db) throw new Error('Database not initialized');

        const col = this.db.collection(collection);
        await col.insert(JSON.stringify(document));
    }

    async deleteDocument(collection: CollectionName, documentId: string): Promise<void> {
        // Rust core binding needs delete method
        // Implementing as placeholder until binding is updated
        console.warn('deleteDocument not yet implemented in native binding');
    }

    async dropCollection(name: CollectionName): Promise<void> {
        // Placeholder
        console.warn('dropCollection not yet implemented in native binding');
    }

    async flushCollection(name: CollectionName): Promise<void> {
        // Native engine handles flushing automatically via WAL/Memtable
        // Explicit flush could be exposed if needed
    }

    async getStats(): Promise<StorageStats> {
        // Placeholder
        return {
            totalDocuments: 0,
            totalSize: 0,
            collections: []
        };
    }
}
