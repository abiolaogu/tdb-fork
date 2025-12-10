/**
 * LumaDB - Luma Database
 *
 * A modern, user-friendly database with multiple query languages
 * and enterprise-grade features.
 *
 * Features:
 * - Multiple Query Languages: LQL (SQL-like), NQL (Natural), JQL (JSON)
 * - ACID Transactions with multiple isolation levels
 * - Advanced Indexing (B-Tree, Hash, Full-Text)
 * - In-memory and persistent storage
 * - Real-time subscriptions
 * - Built-in caching
 * - Comprehensive SDK for multiple languages
 */

// Core exports
export { Database } from './core/Database';
export { Collection } from './core/Collection';
export { Document } from './core/Document';
export { Transaction } from './core/Transaction';

// Storage exports
export { StorageEngine } from './storage/StorageEngine';
export { MemoryStorage } from './storage/MemoryStorage';
export { FileStorage } from './storage/FileStorage';

// Query Language exports
export { QueryEngine } from './query/QueryEngine';
export { LQLParser } from './query/parsers/LQLParser';
export { NQLParser } from './query/parsers/NQLParser';
export { JQLParser } from './query/parsers/JQLParser';

// Index exports
export { IndexManager } from './indexing/IndexManager';
export { BTreeIndex } from './indexing/BTreeIndex';
export { HashIndex } from './indexing/HashIndex';
export { FullTextIndex } from './indexing/FullTextIndex';

// Types
export * from './types';

// Version info
export const VERSION = '1.0.0';
export const CODENAME = 'Aurora';
