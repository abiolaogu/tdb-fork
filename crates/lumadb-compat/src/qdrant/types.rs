//! Qdrant API types - exact compatibility with Qdrant REST API

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Collection Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCollection {
    pub vectors: VectorsConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_number: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_factor: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_consistency_factor: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_disk_payload: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hnsw_config: Option<HnswConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_config: Option<WalConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optimizers_config: Option<OptimizersConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init_from: Option<InitFrom>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantization_config: Option<QuantizationConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VectorsConfig {
    Single(VectorParams),
    Multi(HashMap<String, VectorParams>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorParams {
    pub size: usize,
    pub distance: Distance,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hnsw_config: Option<HnswConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantization_config: Option<QuantizationConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Distance {
    Cosine,
    Euclid,
    Dot,
    Manhattan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub m: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ef_construct: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_scan_threshold: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_indexing_threads: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_m: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_capacity_mb: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_segments_ahead: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizersConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vacuum_min_vector_number: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_segment_number: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_segment_size: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memmap_threshold: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexing_threshold: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_interval_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_optimization_threads: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitFrom {
    pub collection: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QuantizationConfig {
    Scalar(ScalarQuantization),
    Product(ProductQuantization),
    Binary(BinaryQuantization),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalarQuantization {
    #[serde(rename = "scalar")]
    pub config: ScalarQuantizationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalarQuantizationConfig {
    #[serde(rename = "type")]
    pub quantization_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantile: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub always_ram: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantization {
    #[serde(rename = "product")]
    pub config: ProductQuantizationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantizationConfig {
    pub compression: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub always_ram: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryQuantization {
    #[serde(rename = "binary")]
    pub config: BinaryQuantizationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryQuantizationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub always_ram: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub status: CollectionStatus,
    pub optimizer_status: OptimizerStatus,
    pub vectors_count: u64,
    pub indexed_vectors_count: u64,
    pub points_count: u64,
    pub segments_count: usize,
    pub config: CollectionConfig,
    pub payload_schema: HashMap<String, PayloadIndexInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollectionStatus {
    Green,
    Yellow,
    Red,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizerStatus {
    Ok,
    Indexing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig {
    pub params: CollectionParams,
    pub hnsw_config: HnswConfig,
    pub optimizer_config: OptimizersConfig,
    pub wal_config: WalConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantization_config: Option<QuantizationConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionParams {
    pub vectors: VectorsConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_number: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_factor: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_consistency_factor: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_disk_payload: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadIndexInfo {
    pub data_type: PayloadSchemaType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<PayloadSchemaParams>,
    pub points: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadSchemaType {
    Keyword,
    Integer,
    Float,
    Geo,
    Text,
    Bool,
    Datetime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadSchemaParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_token_len: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_token_len: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lowercase: Option<bool>,
}

// ============================================================================
// Point Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PointStruct {
    pub id: PointId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PointId {
    Num(u64),
    Uuid(String),
}

impl ToString for PointId {
    fn to_string(&self) -> String {
        match self {
            PointId::Num(n) => n.to_string(),
            PointId::Uuid(s) => s.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VectorInput {
    Dense(Vec<f32>),
    Named(HashMap<String, Vec<f32>>),
}

pub type Payload = HashMap<String, serde_json::Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertPoints {
    pub points: Vec<PointStruct>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordering: Option<WriteOrdering>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteOrdering {
    Weak,
    Medium,
    Strong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetPoints {
    pub ids: Vec<PointId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_payload: Option<WithPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_vector: Option<WithVector>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WithPayload {
    Bool(bool),
    Fields(Vec<String>),
    Selector(PayloadSelector),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadSelector {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WithVector {
    Bool(bool),
    Fields(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletePoints {
    pub points: PointsSelector,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordering: Option<WriteOrdering>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PointsSelector {
    Ids(Vec<PointId>),
    Filter(Filter),
}

// ============================================================================
// Search Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub vector: NamedVectorStruct,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_payload: Option<WithPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_vector: Option<WithVector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_threshold: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<SearchParams>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NamedVectorStruct {
    Default(Vec<f32>),
    Named { name: String, vector: Vec<f32> },
}

impl NamedVectorStruct {
    pub fn vector(&self) -> &[f32] {
        match self {
            NamedVectorStruct::Default(v) => v,
            NamedVectorStruct::Named { vector, .. } => vector,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hnsw_ef: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exact: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantization: Option<QuantizationSearchParams>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizationSearchParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rescore: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oversampling: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredPoint {
    pub id: PointId,
    pub version: u64,
    pub score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Payload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorInput>,
}

// ============================================================================
// Filter Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub should: Option<Vec<Condition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must: Option<Vec<Condition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub must_not: Option<Vec<Condition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Condition {
    Field(FieldCondition),
    IsEmpty(IsEmptyCondition),
    IsNull(IsNullCondition),
    HasId(HasIdCondition),
    Nested(NestedCondition),
    Filter(Filter),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldCondition {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#match: Option<MatchValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<RangeCondition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geo_bounding_box: Option<GeoBoundingBox>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geo_radius: Option<GeoRadius>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values_count: Option<ValuesCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MatchValue {
    Keyword(MatchKeyword),
    Integer(MatchInteger),
    Bool(MatchBool),
    Text(MatchText),
    Any(MatchAny),
    Except(MatchExcept),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchKeyword {
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchInteger {
    pub value: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchBool {
    pub value: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchText {
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchAny {
    pub any: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchExcept {
    pub except: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeCondition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gt: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gte: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lte: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoBoundingBox {
    pub top_left: GeoPoint,
    pub bottom_right: GeoPoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoRadius {
    pub center: GeoPoint,
    pub radius: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValuesCount {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lt: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gt: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gte: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lte: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsEmptyCondition {
    pub is_empty: IsEmptyKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsEmptyKey {
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsNullCondition {
    pub is_null: IsNullKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsNullKey {
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HasIdCondition {
    pub has_id: Vec<PointId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NestedCondition {
    pub nested: NestedSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NestedSpec {
    pub key: String,
    pub filter: Filter,
}

// ============================================================================
// Response Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QdrantResponse<T> {
    pub result: T,
    pub status: String,
    pub time: f64,
}

impl<T> QdrantResponse<T> {
    pub fn ok(result: T, time: f64) -> Self {
        Self {
            result,
            status: "ok".to_string(),
            time,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QdrantError {
    pub status: QdrantErrorStatus,
    pub time: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QdrantErrorStatus {
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateResult {
    pub operation_id: u64,
    pub status: UpdateStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateStatus {
    Acknowledged,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<PointId>,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_payload: Option<WithPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_vector: Option<WithVector>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollResult {
    pub points: Vec<PointStruct>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_offset: Option<PointId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exact: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountResult {
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionsList {
    pub collections: Vec<CollectionDescription>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionDescription {
    pub name: String,
}
