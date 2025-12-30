//! Query string parsing for PostgREST-compatible filters

use serde::Deserialize;
use std::collections::HashMap;

use supabase_common::error::{Error, Result};

/// Parsed query parameters from request
#[derive(Debug, Clone, Default)]
pub struct ParsedQuery {
    pub select: Option<SelectClause>,
    pub filters: Vec<Filter>,
    pub order: Vec<OrderClause>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub count: Option<CountType>,
}

/// SELECT clause with column selection and nested resources
#[derive(Debug, Clone)]
pub struct SelectClause {
    pub columns: Vec<SelectColumn>,
}

/// Individual column in SELECT
#[derive(Debug, Clone)]
pub struct SelectColumn {
    pub name: String,
    pub alias: Option<String>,
    pub embedded: Option<EmbeddedResource>,
    pub cast: Option<String>,
}

/// Embedded/nested resource for foreign key expansion
#[derive(Debug, Clone)]
pub struct EmbeddedResource {
    pub table: String,
    pub hint: Option<String>,
    pub columns: Vec<SelectColumn>,
    pub filters: Vec<Filter>,
    pub order: Vec<OrderClause>,
    pub limit: Option<usize>,
}

/// Filter condition
#[derive(Debug, Clone)]
pub struct Filter {
    pub column: String,
    pub operator: FilterOperator,
    pub value: FilterValue,
    pub negated: bool,
}

/// Filter operators matching PostgREST
#[derive(Debug, Clone, PartialEq)]
pub enum FilterOperator {
    Eq,     // equals
    Neq,    // not equals
    Gt,     // greater than
    Gte,    // greater than or equal
    Lt,     // less than
    Lte,    // less than or equal
    Like,   // LIKE
    Ilike,  // ILIKE (case-insensitive)
    Match,  // ~ (regex)
    Imatch, // ~* (case-insensitive regex)
    In,     // IN
    Is,     // IS (for NULL, TRUE, FALSE)
    Cs,     // contains (@>)
    Cd,     // contained by (<@)
    Ov,     // overlaps (&&)
    Sl,     // strictly left (<<)
    Sr,     // strictly right (>>)
    Nxl,    // not extends left (&>)
    Nxr,    // not extends right (<&)
    Adj,    // adjacent (-|-)
    Fts,    // full-text search (@@)
    Plfts,  // phrase full-text search
    Phfts,  // plain full-text search
    Wfts,   // websearch full-text search
    Not,    // logical NOT
    And,    // logical AND
    Or,     // logical OR
}

/// Filter value
#[derive(Debug, Clone)]
pub enum FilterValue {
    Single(String),
    List(Vec<String>),
    Null,
    True,
    False,
}

/// Order clause
#[derive(Debug, Clone)]
pub struct OrderClause {
    pub column: String,
    pub direction: OrderDirection,
    pub nulls: NullsOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NullsOrder {
    First,
    Last,
    Default,
}

/// Count type for responses
#[derive(Debug, Clone, PartialEq)]
pub enum CountType {
    Exact,
    Planned,
    Estimated,
}

impl ParsedQuery {
    /// Parse query string into structured query
    pub fn parse(query_string: &str) -> Result<Self> {
        let params: HashMap<String, String> = url::form_urlencoded::parse(query_string.as_bytes())
            .into_owned()
            .collect();

        let mut parsed = ParsedQuery::default();

        // Parse select
        if let Some(select) = params.get("select") {
            parsed.select = Some(parse_select(select)?);
        }

        // Parse order
        if let Some(order) = params.get("order") {
            parsed.order = parse_order(order)?;
        }

        // Parse limit and offset
        if let Some(limit) = params.get("limit") {
            parsed.limit = Some(
                limit
                    .parse()
                    .map_err(|_| Error::InvalidQueryParam("limit".to_string()))?,
            );
        }

        if let Some(offset) = params.get("offset") {
            parsed.offset = Some(
                offset
                    .parse()
                    .map_err(|_| Error::InvalidQueryParam("offset".to_string()))?,
            );
        }

        // Parse filters (all other params are filters)
        for (key, value) in &params {
            if !["select", "order", "limit", "offset", "count"].contains(&key.as_str()) {
                parsed.filters.push(parse_filter(key, value)?);
            }
        }

        Ok(parsed)
    }

    /// Convert to SQL WHERE clause
    pub fn to_sql_where(&self) -> Option<String> {
        if self.filters.is_empty() {
            return None;
        }

        let conditions: Vec<String> = self
            .filters
            .iter()
            .filter_map(|f| filter_to_sql(f))
            .collect();

        if conditions.is_empty() {
            None
        } else {
            Some(conditions.join(" AND "))
        }
    }

    /// Convert to SQL ORDER BY clause
    pub fn to_sql_order(&self) -> Option<String> {
        if self.order.is_empty() {
            return None;
        }

        let clauses: Vec<String> = self
            .order
            .iter()
            .map(|o| {
                let dir = match o.direction {
                    OrderDirection::Asc => "ASC",
                    OrderDirection::Desc => "DESC",
                };
                let nulls = match o.nulls {
                    NullsOrder::First => " NULLS FIRST",
                    NullsOrder::Last => " NULLS LAST",
                    NullsOrder::Default => "",
                };
                format!("\"{}\" {}{}", o.column, dir, nulls)
            })
            .collect();

        Some(clauses.join(", "))
    }
}

/// Parse select clause
fn parse_select(select: &str) -> Result<SelectClause> {
    let mut columns = Vec::new();

    // Simple parser - split by comma, handling nested parentheses
    let parts = split_respecting_parens(select, ',');

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Check for alias (column:alias or table:column)
        if let Some((alias_or_table, rest)) = part.split_once(':') {
            // Check if rest contains embedded resource
            if rest.contains('(') {
                // Embedded resource: alias:table(columns)
                if let Some(paren_start) = rest.find('(') {
                    let table = &rest[..paren_start];
                    let inner = &rest[paren_start + 1..rest.len() - 1];
                    let inner_select = parse_select(inner)?;

                    columns.push(SelectColumn {
                        name: table.to_string(),
                        alias: Some(alias_or_table.to_string()),
                        embedded: Some(EmbeddedResource {
                            table: table.to_string(),
                            hint: None,
                            columns: inner_select.columns,
                            filters: vec![],
                            order: vec![],
                            limit: None,
                        }),
                        cast: None,
                    });
                }
            } else {
                columns.push(SelectColumn {
                    name: rest.to_string(),
                    alias: Some(alias_or_table.to_string()),
                    embedded: None,
                    cast: None,
                });
            }
        } else if part.contains('(') {
            // Embedded resource without alias: table(columns)
            if let Some(paren_start) = part.find('(') {
                let table = &part[..paren_start];
                let inner = &part[paren_start + 1..part.len() - 1];
                let inner_select = parse_select(inner)?;

                columns.push(SelectColumn {
                    name: table.to_string(),
                    alias: None,
                    embedded: Some(EmbeddedResource {
                        table: table.to_string(),
                        hint: None,
                        columns: inner_select.columns,
                        filters: vec![],
                        order: vec![],
                        limit: None,
                    }),
                    cast: None,
                });
            }
        } else {
            // Simple column
            columns.push(SelectColumn {
                name: part.to_string(),
                alias: None,
                embedded: None,
                cast: None,
            });
        }
    }

    Ok(SelectClause { columns })
}

/// Parse order clause
fn parse_order(order: &str) -> Result<Vec<OrderClause>> {
    let mut clauses = Vec::new();

    for part in order.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let segments: Vec<&str> = part.split('.').collect();
        let column = segments[0].to_string();

        let mut direction = OrderDirection::Asc;
        let mut nulls = NullsOrder::Default;

        for segment in segments.iter().skip(1) {
            match *segment {
                "asc" => direction = OrderDirection::Asc,
                "desc" => direction = OrderDirection::Desc,
                "nullsfirst" => nulls = NullsOrder::First,
                "nullslast" => nulls = NullsOrder::Last,
                _ => {}
            }
        }

        clauses.push(OrderClause {
            column,
            direction,
            nulls,
        });
    }

    Ok(clauses)
}

/// Parse a single filter
fn parse_filter(column: &str, value: &str) -> Result<Filter> {
    // Check for negation
    let (column, negated) = if column.starts_with("not.") {
        (&column[4..], true)
    } else {
        (column, false)
    };

    // Parse operator and value
    let (operator, filter_value) = parse_filter_value(value)?;

    Ok(Filter {
        column: column.to_string(),
        operator,
        value: filter_value,
        negated,
    })
}

/// Parse filter value with operator prefix
fn parse_filter_value(value: &str) -> Result<(FilterOperator, FilterValue)> {
    // Check for operator prefix
    let operators = [
        ("eq.", FilterOperator::Eq),
        ("neq.", FilterOperator::Neq),
        ("gt.", FilterOperator::Gt),
        ("gte.", FilterOperator::Gte),
        ("lt.", FilterOperator::Lt),
        ("lte.", FilterOperator::Lte),
        ("like.", FilterOperator::Like),
        ("ilike.", FilterOperator::Ilike),
        ("match.", FilterOperator::Match),
        ("imatch.", FilterOperator::Imatch),
        ("in.", FilterOperator::In),
        ("is.", FilterOperator::Is),
        ("cs.", FilterOperator::Cs),
        ("cd.", FilterOperator::Cd),
        ("ov.", FilterOperator::Ov),
        ("sl.", FilterOperator::Sl),
        ("sr.", FilterOperator::Sr),
        ("nxl.", FilterOperator::Nxl),
        ("nxr.", FilterOperator::Nxr),
        ("adj.", FilterOperator::Adj),
        ("fts.", FilterOperator::Fts),
        ("plfts.", FilterOperator::Plfts),
        ("phfts.", FilterOperator::Phfts),
        ("wfts.", FilterOperator::Wfts),
    ];

    for (prefix, op) in &operators {
        if value.starts_with(prefix) {
            let val = &value[prefix.len()..];
            let filter_val = parse_value_type(val, op)?;
            return Ok((op.clone(), filter_val));
        }
    }

    // Default to eq
    Ok((FilterOperator::Eq, FilterValue::Single(value.to_string())))
}

/// Parse value type based on operator
fn parse_value_type(value: &str, operator: &FilterOperator) -> Result<FilterValue> {
    match operator {
        FilterOperator::Is => match value.to_lowercase().as_str() {
            "null" => Ok(FilterValue::Null),
            "true" => Ok(FilterValue::True),
            "false" => Ok(FilterValue::False),
            _ => Err(Error::InvalidFilter(format!("Invalid IS value: {}", value))),
        },
        FilterOperator::In => {
            // Parse list: (value1,value2,value3)
            if value.starts_with('(') && value.ends_with(')') {
                let inner = &value[1..value.len() - 1];
                let values: Vec<String> = inner.split(',').map(|s| s.trim().to_string()).collect();
                Ok(FilterValue::List(values))
            } else {
                Ok(FilterValue::List(vec![value.to_string()]))
            }
        }
        _ => Ok(FilterValue::Single(value.to_string())),
    }
}

/// Convert filter to SQL condition
fn filter_to_sql(filter: &Filter) -> Option<String> {
    let column = format!("\"{}\"", filter.column);
    let negation = if filter.negated { "NOT " } else { "" };

    let condition = match (&filter.operator, &filter.value) {
        (FilterOperator::Eq, FilterValue::Single(v)) => {
            format!("{} = '{}'", column, escape_sql_string(v))
        }
        (FilterOperator::Neq, FilterValue::Single(v)) => {
            format!("{} != '{}'", column, escape_sql_string(v))
        }
        (FilterOperator::Gt, FilterValue::Single(v)) => {
            format!("{} > '{}'", column, escape_sql_string(v))
        }
        (FilterOperator::Gte, FilterValue::Single(v)) => {
            format!("{} >= '{}'", column, escape_sql_string(v))
        }
        (FilterOperator::Lt, FilterValue::Single(v)) => {
            format!("{} < '{}'", column, escape_sql_string(v))
        }
        (FilterOperator::Lte, FilterValue::Single(v)) => {
            format!("{} <= '{}'", column, escape_sql_string(v))
        }
        (FilterOperator::Like, FilterValue::Single(v)) => {
            format!("{} LIKE '{}'", column, escape_sql_string(v))
        }
        (FilterOperator::Ilike, FilterValue::Single(v)) => {
            format!("{} ILIKE '{}'", column, escape_sql_string(v))
        }
        (FilterOperator::Is, FilterValue::Null) => {
            format!("{} IS NULL", column)
        }
        (FilterOperator::Is, FilterValue::True) => {
            format!("{} IS TRUE", column)
        }
        (FilterOperator::Is, FilterValue::False) => {
            format!("{} IS FALSE", column)
        }
        (FilterOperator::In, FilterValue::List(values)) => {
            let escaped: Vec<String> = values
                .iter()
                .map(|v| format!("'{}'", escape_sql_string(v)))
                .collect();
            format!("{} IN ({})", column, escaped.join(", "))
        }
        _ => return None,
    };

    Some(format!("{}{}", negation, condition))
}

/// Escape SQL string to prevent injection
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Split string by delimiter, respecting parentheses nesting
fn split_respecting_parens(s: &str, delimiter: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0;

    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' => {
                depth -= 1;
                current.push(c);
            }
            c if c == delimiter && depth == 0 => {
                parts.push(current.clone());
                current.clear();
            }
            c => current.push(c),
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_filter() {
        let query = ParsedQuery::parse("name=eq.John").unwrap();
        assert_eq!(query.filters.len(), 1);
        assert_eq!(query.filters[0].column, "name");
        assert_eq!(query.filters[0].operator, FilterOperator::Eq);
    }

    #[test]
    fn test_parse_order() {
        let query = ParsedQuery::parse("order=created_at.desc.nullslast").unwrap();
        assert_eq!(query.order.len(), 1);
        assert_eq!(query.order[0].column, "created_at");
        assert_eq!(query.order[0].direction, OrderDirection::Desc);
        assert_eq!(query.order[0].nulls, NullsOrder::Last);
    }

    #[test]
    fn test_parse_limit_offset() {
        let query = ParsedQuery::parse("limit=10&offset=20").unwrap();
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.offset, Some(20));
    }

    #[test]
    fn test_parse_select() {
        let query = ParsedQuery::parse("select=id,name,author:users(name,email)").unwrap();
        let select = query.select.unwrap();
        assert_eq!(select.columns.len(), 3);
        assert!(select.columns[2].embedded.is_some());
    }

    #[test]
    fn test_filter_to_sql() {
        let filter = Filter {
            column: "status".to_string(),
            operator: FilterOperator::Eq,
            value: FilterValue::Single("active".to_string()),
            negated: false,
        };
        let sql = filter_to_sql(&filter).unwrap();
        assert_eq!(sql, "\"status\" = 'active'");
    }

    #[test]
    fn test_in_filter() {
        let query = ParsedQuery::parse("status=in.(active,pending,done)").unwrap();
        assert_eq!(query.filters.len(), 1);
        if let FilterValue::List(values) = &query.filters[0].value {
            assert_eq!(values.len(), 3);
        } else {
            panic!("Expected list value");
        }
    }
}
