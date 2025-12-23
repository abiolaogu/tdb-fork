//! Query parser for LQL and SQL

use lumadb_common::error::{Result, Error, QueryError};

/// Query parser supporting LQL and SQL
pub struct Parser {
    /// SQL parser
    sql_parser: SqlParser,
}

impl Parser {
    /// Create a new parser
    pub fn new() -> Self {
        Self {
            sql_parser: SqlParser::new(),
        }
    }

    /// Parse a query string
    pub fn parse(&self, query: &str) -> Result<Ast> {
        let query = query.trim();

        // Detect query type
        if query.starts_with("SELECT")
            || query.starts_with("select")
            || query.starts_with("INSERT")
            || query.starts_with("insert")
            || query.starts_with("UPDATE")
            || query.starts_with("update")
            || query.starts_with("DELETE")
            || query.starts_with("delete")
            || query.starts_with("CREATE")
            || query.starts_with("create")
            || query.starts_with("DROP")
            || query.starts_with("drop")
        {
            self.sql_parser.parse(query)
        } else {
            // Try LQL
            self.parse_lql(query)
        }
    }

    /// Parse LQL query
    fn parse_lql(&self, query: &str) -> Result<Ast> {
        // Simplified LQL parser
        let parts: Vec<&str> = query.split_whitespace().collect();

        if parts.is_empty() {
            return Err(Error::Query(QueryError::ParseError("Empty query".to_string())));
        }

        match parts[0].to_uppercase().as_str() {
            "STREAM" => self.parse_stream_query(&parts[1..]),
            "TOPIC" => self.parse_topic_query(&parts[1..]),
            "VECTOR" => self.parse_vector_query(&parts[1..]),
            _ => Err(Error::Query(QueryError::ParseError(format!(
                "Unknown LQL command: {}",
                parts[0]
            )))),
        }
    }

    fn parse_stream_query(&self, parts: &[&str]) -> Result<Ast> {
        // STREAM FROM topic WHERE condition
        Ok(Ast::Stream {
            topic: parts.get(1).unwrap_or(&"").to_string(),
            filter: None,
            limit: None,
        })
    }

    fn parse_topic_query(&self, parts: &[&str]) -> Result<Ast> {
        // TOPIC CREATE/LIST/DELETE
        let action = parts.first().unwrap_or(&"LIST");
        match action.to_uppercase().as_str() {
            "CREATE" => Ok(Ast::TopicCreate {
                name: parts.get(1).unwrap_or(&"").to_string(),
                partitions: 3,
                replication: 1,
            }),
            "LIST" => Ok(Ast::TopicList),
            "DELETE" => Ok(Ast::TopicDelete {
                name: parts.get(1).unwrap_or(&"").to_string(),
            }),
            _ => Ok(Ast::TopicList),
        }
    }

    fn parse_vector_query(&self, parts: &[&str]) -> Result<Ast> {
        // VECTOR SEARCH collection [vector] k
        Ok(Ast::VectorSearch {
            collection: parts.get(1).unwrap_or(&"").to_string(),
            vector: Vec::new(),
            k: 10,
        })
    }
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

/// SQL parser wrapper
struct SqlParser;

impl SqlParser {
    fn new() -> Self {
        Self
    }

    fn parse(&self, query: &str) -> Result<Ast> {
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser as SqlParserLib;

        let dialect = GenericDialect {};
        let statements = SqlParserLib::parse_sql(&dialect, query)
            .map_err(|e| Error::Query(QueryError::ParseError(e.to_string())))?;

        if statements.is_empty() {
            return Err(Error::Query(QueryError::ParseError("No statements".to_string())));
        }

        // Convert to our AST
        let stmt = &statements[0];
        self.convert_statement(stmt)
    }

    fn convert_statement(&self, stmt: &sqlparser::ast::Statement) -> Result<Ast> {
        use sqlparser::ast::Statement;

        match stmt {
            Statement::Query(query) => Ok(Ast::Select {
                columns: vec!["*".to_string()],
                from: self.extract_table_name(query),
                filter: None,
                order_by: None,
                limit: None,
            }),
            Statement::Insert { table_name, columns, .. } => Ok(Ast::Insert {
                table: table_name.to_string(),
                columns: columns.iter().map(|c| c.to_string()).collect(),
                values: Vec::new(),
            }),
            Statement::Update { table, .. } => Ok(Ast::Update {
                table: table.relation.to_string(),
                set: std::collections::HashMap::new(),
                filter: None,
            }),
            Statement::Delete { from, .. } => Ok(Ast::Delete {
                table: from.first().map(|t| t.relation.to_string()).unwrap_or_default(),
                filter: None,
            }),
            Statement::CreateTable { name, .. } => Ok(Ast::CreateTable {
                name: name.to_string(),
                columns: Vec::new(),
            }),
            Statement::Drop { names, .. } => Ok(Ast::DropTable {
                name: names.first().map(|n| n.to_string()).unwrap_or_default(),
            }),
            _ => Err(Error::Query(QueryError::ParseError(
                "Unsupported statement type".to_string(),
            ))),
        }
    }

    fn extract_table_name(&self, query: &sqlparser::ast::Query) -> String {
        // Simplified extraction
        if let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() {
            if let Some(from) = select.from.first() {
                return from.relation.to_string();
            }
        }
        String::new()
    }
}

/// Abstract Syntax Tree
#[derive(Debug, Clone)]
pub enum Ast {
    // SQL statements
    Select {
        columns: Vec<String>,
        from: String,
        filter: Option<Expr>,
        order_by: Option<Vec<(String, bool)>>,
        limit: Option<usize>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<serde_json::Value>>,
    },
    Update {
        table: String,
        set: std::collections::HashMap<String, serde_json::Value>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    DropTable {
        name: String,
    },

    // LQL statements
    Stream {
        topic: String,
        filter: Option<Expr>,
        limit: Option<usize>,
    },
    TopicCreate {
        name: String,
        partitions: u32,
        replication: u32,
    },
    TopicList,
    TopicDelete {
        name: String,
    },
    VectorSearch {
        collection: String,
        vector: Vec<f32>,
        k: usize,
    },
}

/// Expression node
#[derive(Debug, Clone)]
pub enum Expr {
    Column(String),
    Literal(serde_json::Value),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
}

/// Binary operators
#[derive(Debug, Clone, Copy)]
pub enum BinaryOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Plus,
    Minus,
    Mul,
    Div,
}

/// Column definition
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Data types
#[derive(Debug, Clone)]
pub enum DataType {
    Int64,
    Float64,
    String,
    Boolean,
    Timestamp,
    Json,
    Vector(usize),
}
