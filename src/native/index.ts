/*
 * TDB+ Native Bindings
 * 
 * Loads the compiled Rust addon.
 */

import { join } from 'path';

export interface NativeDatabase {
    close(): Promise<void>;
    collection(name: string): NativeCollection;
}

export interface NativeCollection {
    insert(docJson: string): Promise<string>;
    get(id: string): Promise<string | null>;
    scan(): Promise<string[]>;
}

export interface NativeBinding {
    Database: {
        open(path: string): Promise<NativeDatabase>;
    };
}

let nativeBinding: NativeBinding;

try {
    // Try loading from release
    nativeBinding = require('../../rust-core/target/release/tdb_core.node');
} catch (e) {
    try {
        // Try debug
        nativeBinding = require('../../rust-core/target/debug/tdb_core.node');
    } catch (e2) {
        throw new Error('Failed to load TDB+ native binding. Be sure to run "cargo build" in rust-core.');
    }
}

export const { Database } = nativeBinding;
