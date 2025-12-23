use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "query/lql.pest"] // PEST will look for this file relative to src/
pub struct LQLParser;

#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
}

#[derive(Debug)]
pub struct SelectStatement {
    pub fields: Vec<String>,
    pub table: String,
    // where_clause: Option<WhereClause>,
}

#[derive(Debug)]
pub struct InsertStatement {
    pub table: String,
    pub values: Vec<String>,
}

pub fn parse(query: &str) -> Result<Statement, pest::error::Error<Rule>> {
    let pairs = LQLParser::parse(Rule::query, query)?;
    
    // Very simplified AST construction for MVP
    // In real impl, we iterate pairs recursively
    
    Ok(Statement::Select(SelectStatement {
        fields: vec!["*".to_string()],
        table: "unknown".to_string(),
    }))
}
