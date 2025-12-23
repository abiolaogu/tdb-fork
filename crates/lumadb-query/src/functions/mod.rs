//! Built-in and user-defined functions

use std::collections::HashMap;
use std::sync::Arc;

use lumadb_common::error::{Result, Error, QueryError};

/// Function registry
pub struct FunctionRegistry {
    /// Scalar functions
    scalar_functions: HashMap<String, Arc<dyn ScalarFunction>>,
    /// Aggregate functions
    aggregate_functions: HashMap<String, Arc<dyn AggregateFunction>>,
}

impl FunctionRegistry {
    /// Create a new function registry with built-in functions
    pub fn new() -> Self {
        let mut registry = Self {
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
        };

        // Register built-in scalar functions
        registry.register_scalar("abs", Arc::new(AbsFunction));
        registry.register_scalar("upper", Arc::new(UpperFunction));
        registry.register_scalar("lower", Arc::new(LowerFunction));
        registry.register_scalar("length", Arc::new(LengthFunction));
        registry.register_scalar("coalesce", Arc::new(CoalesceFunction));
        registry.register_scalar("now", Arc::new(NowFunction));

        // Register built-in aggregate functions
        registry.register_aggregate("count", Arc::new(CountFunction));
        registry.register_aggregate("sum", Arc::new(SumFunction));
        registry.register_aggregate("avg", Arc::new(AvgFunction));
        registry.register_aggregate("min", Arc::new(MinFunction));
        registry.register_aggregate("max", Arc::new(MaxFunction));

        registry
    }

    /// Register a scalar function
    pub fn register_scalar(&mut self, name: &str, func: Arc<dyn ScalarFunction>) {
        self.scalar_functions.insert(name.to_lowercase(), func);
    }

    /// Register an aggregate function
    pub fn register_aggregate(&mut self, name: &str, func: Arc<dyn AggregateFunction>) {
        self.aggregate_functions.insert(name.to_lowercase(), func);
    }

    /// Get a scalar function
    pub fn get_scalar(&self, name: &str) -> Option<Arc<dyn ScalarFunction>> {
        self.scalar_functions.get(&name.to_lowercase()).cloned()
    }

    /// Get an aggregate function
    pub fn get_aggregate(&self, name: &str) -> Option<Arc<dyn AggregateFunction>> {
        self.aggregate_functions.get(&name.to_lowercase()).cloned()
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Scalar function trait
pub trait ScalarFunction: Send + Sync {
    /// Function name
    fn name(&self) -> &str;

    /// Evaluate the function
    fn evaluate(&self, args: &[serde_json::Value]) -> Result<serde_json::Value>;
}

/// Aggregate function trait
pub trait AggregateFunction: Send + Sync {
    /// Function name
    fn name(&self) -> &str;

    /// Create a new accumulator
    fn create_accumulator(&self) -> Box<dyn Accumulator>;
}

/// Accumulator for aggregate functions
pub trait Accumulator: Send {
    /// Add a value
    fn accumulate(&mut self, value: &serde_json::Value);

    /// Get the final result
    fn finalize(&self) -> serde_json::Value;

    /// Merge with another accumulator
    fn merge(&mut self, other: &dyn Accumulator);

    /// Support downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}

// ============================================================================
// Built-in Scalar Functions
// ============================================================================

struct AbsFunction;

impl ScalarFunction for AbsFunction {
    fn name(&self) -> &str {
        "abs"
    }

    fn evaluate(&self, args: &[serde_json::Value]) -> Result<serde_json::Value> {
        if args.is_empty() {
            return Err(Error::Query(QueryError::UnknownFunction(
                "abs requires 1 argument".to_string(),
            )));
        }

        match &args[0] {
            serde_json::Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    Ok(serde_json::json!(f.abs()))
                } else if let Some(i) = n.as_i64() {
                    Ok(serde_json::json!(i.abs()))
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            _ => Ok(serde_json::Value::Null),
        }
    }
}

struct UpperFunction;

impl ScalarFunction for UpperFunction {
    fn name(&self) -> &str {
        "upper"
    }

    fn evaluate(&self, args: &[serde_json::Value]) -> Result<serde_json::Value> {
        match args.first() {
            Some(serde_json::Value::String(s)) => Ok(serde_json::json!(s.to_uppercase())),
            _ => Ok(serde_json::Value::Null),
        }
    }
}

struct LowerFunction;

impl ScalarFunction for LowerFunction {
    fn name(&self) -> &str {
        "lower"
    }

    fn evaluate(&self, args: &[serde_json::Value]) -> Result<serde_json::Value> {
        match args.first() {
            Some(serde_json::Value::String(s)) => Ok(serde_json::json!(s.to_lowercase())),
            _ => Ok(serde_json::Value::Null),
        }
    }
}

struct LengthFunction;

impl ScalarFunction for LengthFunction {
    fn name(&self) -> &str {
        "length"
    }

    fn evaluate(&self, args: &[serde_json::Value]) -> Result<serde_json::Value> {
        match args.first() {
            Some(serde_json::Value::String(s)) => Ok(serde_json::json!(s.len())),
            Some(serde_json::Value::Array(a)) => Ok(serde_json::json!(a.len())),
            _ => Ok(serde_json::Value::Null),
        }
    }
}

struct CoalesceFunction;

impl ScalarFunction for CoalesceFunction {
    fn name(&self) -> &str {
        "coalesce"
    }

    fn evaluate(&self, args: &[serde_json::Value]) -> Result<serde_json::Value> {
        for arg in args {
            if !arg.is_null() {
                return Ok(arg.clone());
            }
        }
        Ok(serde_json::Value::Null)
    }
}

struct NowFunction;

impl ScalarFunction for NowFunction {
    fn name(&self) -> &str {
        "now"
    }

    fn evaluate(&self, _args: &[serde_json::Value]) -> Result<serde_json::Value> {
        Ok(serde_json::json!(chrono::Utc::now().to_rfc3339()))
    }
}

// ============================================================================
// Built-in Aggregate Functions
// ============================================================================

struct CountFunction;

impl AggregateFunction for CountFunction {
    fn name(&self) -> &str {
        "count"
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountAccumulator { count: 0 })
    }
}

struct CountAccumulator {
    count: u64,
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, _value: &serde_json::Value) {
        self.count += 1;
    }

    fn finalize(&self) -> serde_json::Value {
        serde_json::json!(self.count)
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(count_acc) = other.as_any().downcast_ref::<CountAccumulator>() {
            self.count += count_acc.count;
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct SumFunction;

impl AggregateFunction for SumFunction {
    fn name(&self) -> &str {
        "sum"
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumAccumulator { sum: 0.0 })
    }
}

struct SumAccumulator {
    sum: f64,
}

impl Accumulator for SumAccumulator {
    fn accumulate(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.sum += n;
        }
    }

    fn finalize(&self) -> serde_json::Value {
        serde_json::json!(self.sum)
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(sum_acc) = other.as_any().downcast_ref::<SumAccumulator>() {
            self.sum += sum_acc.sum;
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct AvgFunction;

impl AggregateFunction for AvgFunction {
    fn name(&self) -> &str {
        "avg"
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgAccumulator { sum: 0.0, count: 0 })
    }
}

struct AvgAccumulator {
    sum: f64,
    count: u64,
}

impl Accumulator for AvgAccumulator {
    fn accumulate(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.sum += n;
            self.count += 1;
        }
    }

    fn finalize(&self) -> serde_json::Value {
        if self.count == 0 {
            serde_json::Value::Null
        } else {
            serde_json::json!(self.sum / self.count as f64)
        }
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(avg_acc) = other.as_any().downcast_ref::<AvgAccumulator>() {
            self.sum += avg_acc.sum;
            self.count += avg_acc.count;
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct MinFunction;

impl AggregateFunction for MinFunction {
    fn name(&self) -> &str {
        "min"
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MinAccumulator { min: None })
    }
}

struct MinAccumulator {
    min: Option<f64>,
}

impl Accumulator for MinAccumulator {
    fn accumulate(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.min = Some(self.min.map_or(n, |m| m.min(n)));
        }
    }

    fn finalize(&self) -> serde_json::Value {
        self.min.map_or(serde_json::Value::Null, |m| serde_json::json!(m))
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(min_acc) = other.as_any().downcast_ref::<MinAccumulator>() {
            if let Some(other_min) = min_acc.min {
                self.min = Some(self.min.map_or(other_min, |m| m.min(other_min)));
            }
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

struct MaxFunction;

impl AggregateFunction for MaxFunction {
    fn name(&self) -> &str {
        "max"
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MaxAccumulator { max: None })
    }
}

struct MaxAccumulator {
    max: Option<f64>,
}

impl Accumulator for MaxAccumulator {
    fn accumulate(&mut self, value: &serde_json::Value) {
        if let Some(n) = value.as_f64() {
            self.max = Some(self.max.map_or(n, |m| m.max(n)));
        }
    }

    fn finalize(&self) -> serde_json::Value {
        self.max.map_or(serde_json::Value::Null, |m| serde_json::json!(m))
    }

    fn merge(&mut self, other: &dyn Accumulator) {
        if let Some(max_acc) = other.as_any().downcast_ref::<MaxAccumulator>() {
            if let Some(other_max) = max_acc.max {
                self.max = Some(self.max.map_or(other_max, |m| m.max(other_max)));
            }
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
