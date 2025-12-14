//! PromptQL - AI-Powered Natural Language Queries
//! 
//! Converts natural language questions to SQL using LLM integration

use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tracing::{info, debug, error};

/// PromptQL query request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptQLQuery {
    pub question: String,
    #[serde(default)]
    pub context: QueryContext,
    #[serde(default)]
    pub options: QueryOptions,
}

/// Context for query generation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryContext {
    /// Available tables
    pub tables: Vec<TableSchema>,
    /// Previous queries in conversation
    pub history: Vec<ConversationTurn>,
    /// User hints
    pub hints: Vec<String>,
}

/// Table schema for context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub description: Option<String>,
}

/// Conversation history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationTurn {
    pub question: String,
    pub sql: String,
    pub result_summary: Option<String>,
}

/// Query options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryOptions {
    /// Maximum tokens for response
    pub max_tokens: Option<u32>,
    /// Temperature for generation
    pub temperature: Option<f32>,
    /// Explain the query
    pub explain: bool,
    /// Dry run (don't execute)
    pub dry_run: bool,
}

/// PromptQL result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptQLResult {
    pub original_question: String,
    pub generated_sql: String,
    pub explanation: Option<String>,
    pub confidence: f32,
    pub executed: bool,
    pub data: Option<serde_json::Value>,
    pub error: Option<String>,
}

/// LLM Provider configuration
#[derive(Debug, Clone)]
pub enum LLMProvider {
    OpenAI { api_key: String, model: String },
    Anthropic { api_key: String, model: String },
    Ollama { url: String, model: String },
    Mock,
}

/// PromptQL Engine
pub struct PromptQLEngine {
    provider: LLMProvider,
    system_prompt: String,
}

impl PromptQLEngine {
    pub fn new(provider: LLMProvider) -> Self {
        Self {
            provider,
            system_prompt: Self::default_system_prompt(),
        }
    }
    
    fn default_system_prompt() -> String {
        r#"You are a SQL query generator for LumaDB, a high-performance observability database.

Given a natural language question and schema context, generate a valid SQL query.
Respond with ONLY the SQL query, no explanations or markdown.

Rules:
1. Use proper SQL syntax
2. Only reference tables and columns that exist in the schema
3. Use appropriate aggregations and filters
4. For time-series data, use timestamp columns appropriately
5. Keep queries efficient - avoid SELECT * when possible

Common patterns:
- Metrics: SELECT avg(value) FROM metrics WHERE name = '...' GROUP BY time_bucket('1h', timestamp)
- Logs: SELECT * FROM logs WHERE text_search(message, 'error') ORDER BY timestamp DESC LIMIT 100
- Traces: SELECT * FROM traces WHERE service = '...' AND duration > 1000"#.to_string()
    }
    
    /// Generate SQL from natural language
    pub async fn generate_sql(&self, query: &PromptQLQuery) -> Result<PromptQLResult, String> {
        let prompt = self.build_prompt(query);
        
        let generated_sql = match &self.provider {
            LLMProvider::OpenAI { api_key, model } => {
                self.call_openai(api_key, model, &prompt).await?
            }
            LLMProvider::Anthropic { api_key, model } => {
                self.call_anthropic(api_key, model, &prompt).await?
            }
            LLMProvider::Ollama { url, model } => {
                self.call_ollama(url, model, &prompt).await?
            }
            LLMProvider::Mock => {
                self.mock_generate(&query.question)?
            }
        };
        
        let confidence = self.estimate_confidence(&generated_sql, query);
        
        Ok(PromptQLResult {
            original_question: query.question.clone(),
            generated_sql,
            explanation: if query.options.explain {
                Some(self.generate_explanation(query))
            } else {
                None
            },
            confidence,
            executed: false,
            data: None,
            error: None,
        })
    }
    
    fn build_prompt(&self, query: &PromptQLQuery) -> String {
        let mut prompt = self.system_prompt.clone();
        
        // Add schema context
        if !query.context.tables.is_empty() {
            prompt.push_str("\n\n## Available Tables:\n");
            for table in &query.context.tables {
                prompt.push_str(&format!("\n### {}\n", table.name));
                if let Some(desc) = &table.description {
                    prompt.push_str(&format!("Description: {}\n", desc));
                }
                prompt.push_str("Columns:\n");
                for col in &table.columns {
                    prompt.push_str(&format!("- {} ({})", col.name, col.data_type));
                    if let Some(desc) = &col.description {
                        prompt.push_str(&format!(": {}", desc));
                    }
                    prompt.push('\n');
                }
            }
        }
        
        // Add conversation history
        if !query.context.history.is_empty() {
            prompt.push_str("\n\n## Previous Queries:\n");
            for turn in &query.context.history {
                prompt.push_str(&format!("Q: {}\nSQL: {}\n\n", turn.question, turn.sql));
            }
        }
        
        // Add current question
        prompt.push_str(&format!("\n\n## Current Question:\n{}\n\n## SQL Query:", query.question));
        
        prompt
    }
    
    async fn call_openai(&self, api_key: &str, model: &str, prompt: &str) -> Result<String, String> {
        let client = reqwest::Client::new();
        
        let response = client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "model": model,
                "messages": [
                    {"role": "system", "content": &self.system_prompt},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 500,
                "temperature": 0.1
            }))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        
        let json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
        
        json["choices"][0]["message"]["content"]
            .as_str()
            .map(|s| s.trim().to_string())
            .ok_or_else(|| "Failed to parse response".to_string())
    }
    
    async fn call_anthropic(&self, api_key: &str, model: &str, prompt: &str) -> Result<String, String> {
        let client = reqwest::Client::new();
        
        let response = client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "model": model,
                "max_tokens": 500,
                "system": &self.system_prompt,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        
        let json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
        
        json["content"][0]["text"]
            .as_str()
            .map(|s| s.trim().to_string())
            .ok_or_else(|| "Failed to parse response".to_string())
    }
    
    async fn call_ollama(&self, url: &str, model: &str, prompt: &str) -> Result<String, String> {
        let client = reqwest::Client::new();
        
        let response = client
            .post(&format!("{}/api/generate", url))
            .json(&serde_json::json!({
                "model": model,
                "prompt": prompt,
                "stream": false
            }))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        
        let json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
        
        json["response"]
            .as_str()
            .map(|s| s.trim().to_string())
            .ok_or_else(|| "Failed to parse response".to_string())
    }
    
    fn mock_generate(&self, question: &str) -> Result<String, String> {
        let question_lower = question.to_lowercase();
        
        // Simple pattern matching for demo
        if question_lower.contains("error") || question_lower.contains("log") {
            Ok("SELECT * FROM logs WHERE level = 'error' ORDER BY timestamp DESC LIMIT 100".to_string())
        } else if question_lower.contains("cpu") || question_lower.contains("memory") || question_lower.contains("metric") {
            Ok("SELECT time_bucket('5m', timestamp) as bucket, avg(value) FROM metrics WHERE name LIKE '%cpu%' GROUP BY bucket ORDER BY bucket DESC LIMIT 100".to_string())
        } else if question_lower.contains("slow") || question_lower.contains("latency") || question_lower.contains("trace") {
            Ok("SELECT * FROM traces WHERE duration_ms > 1000 ORDER BY duration_ms DESC LIMIT 50".to_string())
        } else if question_lower.contains("count") {
            Ok("SELECT COUNT(*) FROM metrics".to_string())
        } else {
            Ok("SELECT * FROM metrics LIMIT 100".to_string())
        }
    }
    
    fn estimate_confidence(&self, sql: &str, query: &PromptQLQuery) -> f32 {
        let mut confidence: f32 = 0.5;
        
        // Higher confidence if tables are in context
        for table in &query.context.tables {
            if sql.to_lowercase().contains(&table.name.to_lowercase()) {
                confidence += 0.1;
            }
        }
        
        // SQL syntax indicators
        if sql.to_uppercase().starts_with("SELECT") {
            confidence += 0.1;
        }
        if sql.contains("FROM") {
            confidence += 0.1;
        }
        
        f32::min(confidence, 0.95)
    }
    
    fn generate_explanation(&self, query: &PromptQLQuery) -> String {
        format!("This query was generated from: '{}'\nIt will search the available tables and return relevant data.", query.question)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_mock_generation() {
        let engine = PromptQLEngine::new(LLMProvider::Mock);
        
        let query = PromptQLQuery {
            question: "Show me CPU usage".to_string(),
            context: QueryContext::default(),
            options: QueryOptions::default(),
        };
        
        let result = engine.generate_sql(&query).await.unwrap();
        assert!(result.generated_sql.contains("metrics"));
        assert!(result.generated_sql.contains("cpu"));
    }
}
