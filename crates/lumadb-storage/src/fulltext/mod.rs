//! Full-text search using Tantivy

use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, STORED, TEXT, Field, Value};
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};
use tracing::info;

use lumadb_common::error::{Result, Error};

/// Full-text search index
pub struct FullTextIndex {
    /// Tantivy index
    index: Index,
    /// Index reader
    reader: IndexReader,
    /// Index writer
    writer: RwLock<IndexWriter>,
    /// Schema
    schema: Schema,
    /// ID field
    id_field: Field,
    /// Content field
    content_field: Field,
}

impl FullTextIndex {
    /// Create a new full-text index
    pub fn new(path: &Path) -> Result<Self> {
        info!("Creating full-text index at {:?}", path);

        // Create schema
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", STORED);
        let content_field = schema_builder.add_text_field("content", TEXT | STORED);
        let schema = schema_builder.build();

        // Create or open index
        std::fs::create_dir_all(path)?;

        let index = Index::create_in_dir(path, schema.clone())
            .or_else(|_| Index::open_in_dir(path))
            .map_err(|e| Error::Internal(format!("Failed to open index: {}", e)))?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .map_err(|e| Error::Internal(format!("Failed to create reader: {}", e)))?;

        let writer = index
            .writer(50_000_000)
            .map_err(|e| Error::Internal(format!("Failed to create writer: {}", e)))?;

        Ok(Self {
            index,
            reader,
            writer: RwLock::new(writer),
            schema,
            id_field,
            content_field,
        })
    }

    /// Index a document
    pub fn index_document(&self, id: &str, content: &str) -> Result<()> {
        let mut writer = self.writer.write();

        writer.add_document(doc!(
            self.id_field => id,
            self.content_field => content
        )).map_err(|e| Error::Internal(format!("Failed to index document: {}", e)))?;

        Ok(())
    }

    /// Commit pending changes
    pub fn commit(&self) -> Result<()> {
        let mut writer = self.writer.write();
        writer.commit()
            .map_err(|e| Error::Internal(format!("Failed to commit: {}", e)))?;
        Ok(())
    }

    /// Search for documents
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        let searcher = self.reader.searcher();

        let query_parser = QueryParser::for_index(&self.index, vec![self.content_field]);
        let query = query_parser
            .parse_query(query)
            .map_err(|e| Error::Internal(format!("Failed to parse query: {}", e)))?;

        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(limit))
            .map_err(|e| Error::Internal(format!("Search failed: {}", e)))?;

        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)
                .map_err(|e| Error::Internal(format!("Failed to retrieve doc: {}", e)))?;

            let id = doc
                .get_first(self.id_field)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default();

            let content = doc
                .get_first(self.content_field)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default();

            results.push(SearchResult { id, content, score });
        }

        Ok(results)
    }

    /// Delete a document by ID
    pub fn delete(&self, id: &str) -> Result<()> {
        let mut writer = self.writer.write();
        let term = tantivy::Term::from_field_text(self.id_field, id);
        writer.delete_term(term);
        Ok(())
    }
}

/// Full-text search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: String,
    pub content: String,
    pub score: f32,
}
