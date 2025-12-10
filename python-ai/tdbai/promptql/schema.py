"""
Schema Inference - Automatic schema detection and relationship discovery.

This module implements:
- Schema inference from data samples
- Relationship detection between collections
- Data type detection
- Semantic field classification
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from enum import Enum
from datetime import datetime
import re
import statistics


class DataType(Enum):
    """Detected data types."""
    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    DATE = "date"
    TIME = "time"
    UUID = "uuid"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    JSON = "json"
    ARRAY = "array"
    OBJECT = "object"
    BINARY = "binary"
    NULL = "null"
    UNKNOWN = "unknown"


class SemanticType(Enum):
    """Semantic classification of fields."""
    IDENTIFIER = "identifier"       # Primary keys, IDs
    NAME = "name"                   # Names of entities
    DESCRIPTION = "description"     # Text descriptions
    AMOUNT = "amount"               # Money, quantities
    COUNT = "count"                 # Counts
    PERCENTAGE = "percentage"       # Percentages
    TIMESTAMP = "timestamp"         # Event timestamps
    CREATED_AT = "created_at"       # Creation time
    UPDATED_AT = "updated_at"       # Update time
    FOREIGN_KEY = "foreign_key"     # References to other entities
    EMAIL = "email"                 # Email addresses
    PHONE = "phone"                 # Phone numbers
    ADDRESS = "address"             # Addresses
    STATUS = "status"               # Status fields
    TYPE = "type"                   # Type/category fields
    FLAG = "flag"                   # Boolean flags
    SCORE = "score"                 # Scores/ratings
    COORDINATE = "coordinate"       # Geographic coordinates
    GENERIC = "generic"             # Unknown semantic type


class RelationshipType(Enum):
    """Types of relationships between entities."""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


@dataclass
class FieldInfo:
    """Information about a field."""
    name: str
    data_type: DataType
    semantic_type: SemanticType
    nullable: bool = True
    unique: bool = False
    indexed: bool = False
    sample_values: List[Any] = field(default_factory=list)
    statistics: Dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass
class Relationship:
    """Relationship between two collections."""
    source_collection: str
    source_field: str
    target_collection: str
    target_field: str
    relationship_type: RelationshipType
    confidence: float
    inferred: bool = True


@dataclass
class InferredSchema:
    """Inferred schema for a collection."""
    collection: str
    fields: Dict[str, FieldInfo]
    primary_key: Optional[str]
    relationships: List[Relationship]
    row_count: int = 0
    sample_size: int = 0


class SchemaInference:
    """
    Automatically infers schema from data samples.

    Features:
    - Data type detection
    - Semantic classification
    - Primary key detection
    - Statistics gathering
    """

    # Patterns for semantic type detection
    ID_PATTERNS = [
        r"^id$", r"_id$", r"^.*_id$", r"^uuid$", r"^guid$",
        r"^pk$", r"^key$", r"^.*_key$",
    ]

    NAME_PATTERNS = [
        r"^name$", r"_name$", r"^.*_name$", r"^title$",
        r"^first_?name$", r"^last_?name$", r"^full_?name$",
    ]

    TIMESTAMP_PATTERNS = [
        r"^created", r"^updated", r"^modified", r"^deleted",
        r"_at$", r"_time$", r"_date$", r"^timestamp",
    ]

    EMAIL_PATTERN = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    URL_PATTERN = r"^https?://"
    PHONE_PATTERN = r"^\+?[0-9]{10,15}$"

    def __init__(self, sample_size: int = 1000):
        self.sample_size = sample_size

    def infer(
        self,
        collection: str,
        data: List[Dict[str, Any]],
    ) -> InferredSchema:
        """
        Infer schema from data samples.

        Args:
            collection: Collection name
            data: Sample data records

        Returns:
            Inferred schema
        """
        if not data:
            return InferredSchema(
                collection=collection,
                fields={},
                primary_key=None,
                relationships=[],
            )

        # Sample data if too large
        sample = data[:self.sample_size]

        # Collect field values
        field_values: Dict[str, List[Any]] = {}
        for record in sample:
            for key, value in record.items():
                if key not in field_values:
                    field_values[key] = []
                field_values[key].append(value)

        # Infer each field
        fields = {}
        for field_name, values in field_values.items():
            fields[field_name] = self._infer_field(field_name, values)

        # Detect primary key
        primary_key = self._detect_primary_key(fields, len(sample))

        return InferredSchema(
            collection=collection,
            fields=fields,
            primary_key=primary_key,
            relationships=[],
            row_count=len(data),
            sample_size=len(sample),
        )

    def _infer_field(self, name: str, values: List[Any]) -> FieldInfo:
        """Infer information about a field."""
        # Filter out None values
        non_null = [v for v in values if v is not None]

        # Detect data type
        data_type = self._detect_data_type(non_null)

        # Detect semantic type
        semantic_type = self._detect_semantic_type(name, non_null, data_type)

        # Calculate statistics
        stats = self._calculate_statistics(non_null, data_type)

        # Check uniqueness
        unique = len(set(str(v) for v in non_null)) == len(non_null) if non_null else False

        # Sample values (up to 5)
        sample_values = list(set(non_null))[:5]

        return FieldInfo(
            name=name,
            data_type=data_type,
            semantic_type=semantic_type,
            nullable=len(non_null) < len(values),
            unique=unique,
            sample_values=sample_values,
            statistics=stats,
        )

    def _detect_data_type(self, values: List[Any]) -> DataType:
        """Detect the data type from values."""
        if not values:
            return DataType.NULL

        # Count types
        type_counts: Dict[DataType, int] = {}

        for value in values:
            detected = self._classify_value_type(value)
            type_counts[detected] = type_counts.get(detected, 0) + 1

        # Return most common type
        if type_counts:
            return max(type_counts.items(), key=lambda x: x[1])[0]
        return DataType.UNKNOWN

    def _classify_value_type(self, value: Any) -> DataType:
        """Classify a single value's type."""
        if value is None:
            return DataType.NULL

        if isinstance(value, bool):
            return DataType.BOOLEAN

        if isinstance(value, int):
            return DataType.INTEGER

        if isinstance(value, float):
            return DataType.FLOAT

        if isinstance(value, datetime):
            return DataType.DATETIME

        if isinstance(value, (list, tuple)):
            return DataType.ARRAY

        if isinstance(value, dict):
            return DataType.OBJECT

        if isinstance(value, bytes):
            return DataType.BINARY

        # String analysis
        if isinstance(value, str):
            return self._classify_string_type(value)

        return DataType.UNKNOWN

    def _classify_string_type(self, value: str) -> DataType:
        """Classify string value to more specific type."""
        value_lower = value.lower()

        # Check boolean strings
        if value_lower in ("true", "false", "yes", "no", "1", "0"):
            return DataType.BOOLEAN

        # Check UUID
        if re.match(self.UUID_PATTERN, value_lower):
            return DataType.UUID

        # Check email
        if re.match(self.EMAIL_PATTERN, value):
            return DataType.EMAIL

        # Check URL
        if re.match(self.URL_PATTERN, value):
            return DataType.URL

        # Check phone
        if re.match(self.PHONE_PATTERN, re.sub(r"[^\d+]", "", value)):
            return DataType.PHONE

        # Check datetime
        if self._is_datetime_string(value):
            return DataType.DATETIME

        # Check date
        if self._is_date_string(value):
            return DataType.DATE

        # Check if numeric string
        try:
            int(value)
            return DataType.INTEGER
        except ValueError:
            pass

        try:
            float(value)
            return DataType.FLOAT
        except ValueError:
            pass

        return DataType.STRING

    def _is_datetime_string(self, value: str) -> bool:
        """Check if string looks like a datetime."""
        datetime_patterns = [
            r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}",
            r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}",
        ]
        return any(re.match(p, value) for p in datetime_patterns)

    def _is_date_string(self, value: str) -> bool:
        """Check if string looks like a date."""
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",
            r"^\d{2}/\d{2}/\d{4}$",
            r"^\d{2}-\d{2}-\d{4}$",
        ]
        return any(re.match(p, value) for p in date_patterns)

    def _detect_semantic_type(
        self,
        name: str,
        values: List[Any],
        data_type: DataType,
    ) -> SemanticType:
        """Detect semantic meaning of a field."""
        name_lower = name.lower()

        # Check ID patterns
        for pattern in self.ID_PATTERNS:
            if re.match(pattern, name_lower):
                return SemanticType.IDENTIFIER

        # Check name patterns
        for pattern in self.NAME_PATTERNS:
            if re.match(pattern, name_lower):
                return SemanticType.NAME

        # Check timestamp patterns
        for pattern in self.TIMESTAMP_PATTERNS:
            if re.search(pattern, name_lower):
                if "created" in name_lower:
                    return SemanticType.CREATED_AT
                if "updated" in name_lower or "modified" in name_lower:
                    return SemanticType.UPDATED_AT
                return SemanticType.TIMESTAMP

        # Based on data type
        if data_type == DataType.EMAIL:
            return SemanticType.EMAIL

        if data_type == DataType.BOOLEAN:
            return SemanticType.FLAG

        # Check field name hints
        if any(x in name_lower for x in ["status", "state"]):
            return SemanticType.STATUS

        if any(x in name_lower for x in ["type", "category", "kind"]):
            return SemanticType.TYPE

        if any(x in name_lower for x in ["amount", "price", "cost", "total", "fee"]):
            return SemanticType.AMOUNT

        if any(x in name_lower for x in ["count", "num", "quantity", "qty"]):
            return SemanticType.COUNT

        if any(x in name_lower for x in ["percent", "ratio", "rate"]):
            return SemanticType.PERCENTAGE

        if any(x in name_lower for x in ["score", "rating", "rank"]):
            return SemanticType.SCORE

        if any(x in name_lower for x in ["description", "desc", "comment", "note"]):
            return SemanticType.DESCRIPTION

        if any(x in name_lower for x in ["address", "street", "city", "zip"]):
            return SemanticType.ADDRESS

        if any(x in name_lower for x in ["lat", "lng", "longitude", "latitude"]):
            return SemanticType.COORDINATE

        # Check for foreign key pattern
        if name_lower.endswith("_id") and not name_lower == "id":
            return SemanticType.FOREIGN_KEY

        return SemanticType.GENERIC

    def _calculate_statistics(
        self,
        values: List[Any],
        data_type: DataType,
    ) -> Dict[str, Any]:
        """Calculate statistics for values."""
        stats: Dict[str, Any] = {
            "count": len(values),
            "distinct": len(set(str(v) for v in values)),
        }

        if not values:
            return stats

        # Numeric statistics
        if data_type in (DataType.INTEGER, DataType.FLOAT):
            numeric = [float(v) for v in values if self._is_numeric(v)]
            if numeric:
                stats["min"] = min(numeric)
                stats["max"] = max(numeric)
                stats["mean"] = statistics.mean(numeric)
                if len(numeric) > 1:
                    stats["stddev"] = statistics.stdev(numeric)

        # String statistics
        elif data_type == DataType.STRING:
            lengths = [len(str(v)) for v in values]
            stats["min_length"] = min(lengths)
            stats["max_length"] = max(lengths)
            stats["avg_length"] = statistics.mean(lengths)

        return stats

    def _is_numeric(self, value: Any) -> bool:
        """Check if value is numeric."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _detect_primary_key(
        self,
        fields: Dict[str, FieldInfo],
        sample_size: int,
    ) -> Optional[str]:
        """Detect likely primary key field."""
        candidates = []

        for name, info in fields.items():
            score = 0

            # Unique values are required
            if not info.unique:
                continue

            # Not nullable
            if not info.nullable:
                score += 2

            # ID semantic type
            if info.semantic_type == SemanticType.IDENTIFIER:
                score += 5

            # UUID or integer type
            if info.data_type in (DataType.UUID, DataType.INTEGER):
                score += 2

            # Name contains 'id'
            if "id" in name.lower():
                score += 3

            if score > 0:
                candidates.append((name, score))

        if candidates:
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[0][0]

        return None


class RelationshipDetector:
    """
    Detects relationships between collections.

    Uses:
    - Foreign key naming conventions
    - Value overlap analysis
    - Schema structure analysis
    """

    def __init__(self, min_confidence: float = 0.5):
        self.min_confidence = min_confidence

    def detect(
        self,
        schemas: Dict[str, InferredSchema],
        samples: Optional[Dict[str, List[Dict]]] = None,
    ) -> List[Relationship]:
        """
        Detect relationships between collections.

        Args:
            schemas: Dictionary of collection -> InferredSchema
            samples: Optional sample data for value overlap analysis

        Returns:
            List of detected relationships
        """
        relationships = []

        # Method 1: Foreign key naming conventions
        fk_relationships = self._detect_by_naming(schemas)
        relationships.extend(fk_relationships)

        # Method 2: Value overlap (if samples provided)
        if samples:
            overlap_relationships = self._detect_by_value_overlap(schemas, samples)
            # Add only if not already found by naming
            existing = set(
                (r.source_collection, r.source_field, r.target_collection)
                for r in relationships
            )
            for rel in overlap_relationships:
                key = (rel.source_collection, rel.source_field, rel.target_collection)
                if key not in existing:
                    relationships.append(rel)

        return relationships

    def _detect_by_naming(
        self,
        schemas: Dict[str, InferredSchema],
    ) -> List[Relationship]:
        """Detect relationships by foreign key naming conventions."""
        relationships = []
        collection_names = set(schemas.keys())

        for coll_name, schema in schemas.items():
            for field_name, field_info in schema.fields.items():
                # Check for foreign key pattern
                if field_info.semantic_type != SemanticType.FOREIGN_KEY:
                    continue

                # Extract referenced collection name
                # e.g., "user_id" -> "user", "users"
                ref_name = field_name.replace("_id", "").replace("Id", "")

                # Find matching collection
                for target_coll in collection_names:
                    if target_coll == coll_name:
                        continue

                    target_lower = target_coll.lower()
                    ref_lower = ref_name.lower()

                    # Check for match (singular/plural)
                    if (target_lower == ref_lower or
                        target_lower == ref_lower + "s" or
                        target_lower + "s" == ref_lower):

                        # Find target primary key
                        target_schema = schemas[target_coll]
                        target_field = target_schema.primary_key or "id"

                        relationships.append(Relationship(
                            source_collection=coll_name,
                            source_field=field_name,
                            target_collection=target_coll,
                            target_field=target_field,
                            relationship_type=RelationshipType.MANY_TO_ONE,
                            confidence=0.8,
                            inferred=True,
                        ))
                        break

        return relationships

    def _detect_by_value_overlap(
        self,
        schemas: Dict[str, InferredSchema],
        samples: Dict[str, List[Dict]],
    ) -> List[Relationship]:
        """Detect relationships by analyzing value overlap."""
        relationships = []

        # Build value sets for potential key fields
        value_sets: Dict[str, Dict[str, Set]] = {}  # collection -> field -> values

        for coll_name, schema in schemas.items():
            if coll_name not in samples:
                continue

            value_sets[coll_name] = {}

            for field_name, field_info in schema.fields.items():
                # Only check ID-like fields
                if field_info.semantic_type not in (
                    SemanticType.IDENTIFIER,
                    SemanticType.FOREIGN_KEY,
                ):
                    continue

                # Collect values
                values = set()
                for record in samples[coll_name]:
                    if field_name in record and record[field_name] is not None:
                        values.add(str(record[field_name]))

                if values:
                    value_sets[coll_name][field_name] = values

        # Compare value sets between collections
        for source_coll, source_fields in value_sets.items():
            for source_field, source_values in source_fields.items():
                for target_coll, target_fields in value_sets.items():
                    if source_coll == target_coll:
                        continue

                    for target_field, target_values in target_fields.items():
                        # Calculate overlap
                        overlap = len(source_values & target_values)
                        if overlap == 0:
                            continue

                        # Calculate confidence based on overlap
                        source_ratio = overlap / len(source_values)
                        target_ratio = overlap / len(target_values)

                        if source_ratio < self.min_confidence and target_ratio < self.min_confidence:
                            continue

                        # Determine relationship type
                        if source_ratio > 0.9 and target_ratio > 0.9:
                            rel_type = RelationshipType.ONE_TO_ONE
                        elif source_ratio > 0.5:
                            rel_type = RelationshipType.MANY_TO_ONE
                        else:
                            rel_type = RelationshipType.ONE_TO_MANY

                        confidence = max(source_ratio, target_ratio)

                        relationships.append(Relationship(
                            source_collection=source_coll,
                            source_field=source_field,
                            target_collection=target_coll,
                            target_field=target_field,
                            relationship_type=rel_type,
                            confidence=confidence,
                            inferred=True,
                        ))

        return relationships

    def validate_relationship(
        self,
        relationship: Relationship,
        source_data: List[Dict],
        target_data: List[Dict],
    ) -> Tuple[bool, float]:
        """
        Validate a relationship against actual data.

        Returns:
            (is_valid, confidence)
        """
        # Get source values
        source_values = set()
        for record in source_data:
            val = record.get(relationship.source_field)
            if val is not None:
                source_values.add(str(val))

        # Get target values
        target_values = set()
        for record in target_data:
            val = record.get(relationship.target_field)
            if val is not None:
                target_values.add(str(val))

        if not source_values or not target_values:
            return False, 0.0

        # Check if source values exist in target
        matches = len(source_values & target_values)
        coverage = matches / len(source_values)

        return coverage >= self.min_confidence, coverage


class SchemaCache:
    """
    Caches inferred schemas for reuse.
    """

    def __init__(self, ttl_seconds: int = 3600):
        self.cache: Dict[str, Tuple[InferredSchema, datetime]] = {}
        self.ttl_seconds = ttl_seconds

    def get(self, collection: str) -> Optional[InferredSchema]:
        """Get cached schema."""
        if collection not in self.cache:
            return None

        schema, timestamp = self.cache[collection]
        age = (datetime.now() - timestamp).total_seconds()

        if age > self.ttl_seconds:
            del self.cache[collection]
            return None

        return schema

    def put(self, collection: str, schema: InferredSchema):
        """Cache a schema."""
        self.cache[collection] = (schema, datetime.now())

    def invalidate(self, collection: str):
        """Invalidate cached schema."""
        self.cache.pop(collection, None)

    def clear(self):
        """Clear all cached schemas."""
        self.cache.clear()
