"""
Python bindings to the Rust TDB+ storage engine via ctypes.

Provides a Pythonic interface to the high-performance Rust core.
"""

import ctypes
import json
import os
from ctypes import (
    POINTER, Structure, byref, c_char_p, c_int, c_size_t, c_uint64, c_uint8, c_void_p
)
from pathlib import Path
from typing import Any, Dict, List, Optional


class TdbBuffer(Structure):
    """Buffer structure for FFI data transfer."""
    _fields_ = [
        ("data", POINTER(c_uint8)),
        ("len", c_size_t),
        ("capacity", c_size_t),
    ]

    def to_bytes(self) -> bytes:
        """Convert buffer to Python bytes."""
        if not self.data or self.len == 0:
            return b""
        return bytes(self.data[:self.len])

    def to_string(self) -> str:
        """Convert buffer to Python string."""
        return self.to_bytes().decode("utf-8")

    def to_json(self) -> Any:
        """Parse buffer as JSON."""
        return json.loads(self.to_string())


class TdbError(Exception):
    """Exception for TDB+ errors."""

    ERROR_CODES = {
        -1: "Invalid handle",
        -2: "Invalid argument",
        -3: "Not found",
        -4: "Already exists",
        -5: "I/O error",
        -6: "Data corruption",
        -7: "Storage full",
        -8: "Internal error",
    }

    def __init__(self, code: int):
        self.code = code
        message = self.ERROR_CODES.get(code, f"Unknown error ({code})")
        super().__init__(message)


def _check_result(result: int) -> None:
    """Check FFI result and raise exception if error."""
    if result != 0:
        raise TdbError(result)


def _find_library() -> str:
    """Find the TDB+ shared library."""
    # Search paths
    search_paths = [
        # Local development
        Path(__file__).parent.parent.parent.parent / "rust-core" / "target" / "release",
        Path(__file__).parent.parent.parent.parent / "rust-core" / "target" / "debug",
        # Installed
        Path("/usr/local/lib"),
        Path("/usr/lib"),
    ]

    lib_names = {
        "linux": "libtdb_core.so",
        "darwin": "libtdb_core.dylib",
        "win32": "tdb_core.dll",
    }

    import sys
    lib_name = lib_names.get(sys.platform, "libtdb_core.so")

    for path in search_paths:
        lib_path = path / lib_name
        if lib_path.exists():
            return str(lib_path)

    # Try loading from system path
    return lib_name


class TdbCore:
    """
    Low-level bindings to the Rust TDB+ storage engine.

    This class provides direct access to the FFI functions.
    For a higher-level interface, use the Database class.
    """

    _lib: Optional[ctypes.CDLL] = None
    _initialized = False

    @classmethod
    def _ensure_initialized(cls) -> ctypes.CDLL:
        """Ensure the library is loaded."""
        if cls._lib is None:
            lib_path = _find_library()
            cls._lib = ctypes.CDLL(lib_path)
            cls._setup_functions()
            cls._initialized = True
        return cls._lib

    @classmethod
    def _setup_functions(cls) -> None:
        """Set up function signatures."""
        lib = cls._lib

        # tdb_open
        lib.tdb_open.argtypes = [c_char_p, c_char_p, POINTER(c_uint64)]
        lib.tdb_open.restype = c_int

        # tdb_close
        lib.tdb_close.argtypes = [c_uint64]
        lib.tdb_close.restype = c_int

        # tdb_create_collection
        lib.tdb_create_collection.argtypes = [c_uint64, c_char_p]
        lib.tdb_create_collection.restype = c_int

        # tdb_drop_collection
        lib.tdb_drop_collection.argtypes = [c_uint64, c_char_p]
        lib.tdb_drop_collection.restype = c_int

        # tdb_insert
        lib.tdb_insert.argtypes = [c_uint64, c_char_p, c_char_p, POINTER(TdbBuffer)]
        lib.tdb_insert.restype = c_int

        # tdb_get
        lib.tdb_get.argtypes = [c_uint64, c_char_p, c_char_p, POINTER(TdbBuffer)]
        lib.tdb_get.restype = c_int

        # tdb_update
        lib.tdb_update.argtypes = [c_uint64, c_char_p, c_char_p, c_char_p]
        lib.tdb_update.restype = c_int

        # tdb_delete
        lib.tdb_delete.argtypes = [c_uint64, c_char_p, c_char_p]
        lib.tdb_delete.restype = c_int

        # tdb_query
        lib.tdb_query.argtypes = [c_uint64, c_char_p, c_char_p, POINTER(TdbBuffer)]
        lib.tdb_query.restype = c_int

        # tdb_batch_insert
        lib.tdb_batch_insert.argtypes = [c_uint64, c_char_p, c_char_p, POINTER(c_size_t)]
        lib.tdb_batch_insert.restype = c_int

        # tdb_buffer_free
        lib.tdb_buffer_free.argtypes = [POINTER(TdbBuffer)]
        lib.tdb_buffer_free.restype = None

        # tdb_stats
        lib.tdb_stats.argtypes = [c_uint64, POINTER(TdbBuffer)]
        lib.tdb_stats.restype = c_int

        # tdb_version
        lib.tdb_version.argtypes = []
        lib.tdb_version.restype = c_char_p


class Database:
    """
    High-level Python interface to TDB+ storage engine.

    Example:
        db = Database.open("./mydata")
        db.create_collection("users")
        doc_id = db.insert("users", {"name": "Alice", "age": 30})
        user = db.get("users", doc_id)
        db.close()
    """

    def __init__(self, handle: int):
        self._handle = handle
        self._lib = TdbCore._ensure_initialized()
        self._closed = False

    @classmethod
    def open(cls, path: str, config: Optional[Dict] = None) -> "Database":
        """Open a database at the given path."""
        lib = TdbCore._ensure_initialized()

        c_path = path.encode("utf-8")
        c_config = json.dumps(config).encode("utf-8") if config else None

        handle = c_uint64()
        result = lib.tdb_open(c_path, c_config, byref(handle))
        _check_result(result)

        return cls(handle.value)

    def close(self) -> None:
        """Close the database."""
        if self._closed:
            return

        result = self._lib.tdb_close(self._handle)
        self._closed = True
        _check_result(result)

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def create_collection(self, name: str) -> None:
        """Create a new collection."""
        result = self._lib.tdb_create_collection(
            self._handle,
            name.encode("utf-8")
        )
        _check_result(result)

    def drop_collection(self, name: str) -> None:
        """Drop a collection."""
        result = self._lib.tdb_drop_collection(
            self._handle,
            name.encode("utf-8")
        )
        _check_result(result)

    def insert(self, collection: str, document: Dict) -> str:
        """Insert a document and return its ID."""
        id_buf = TdbBuffer()

        result = self._lib.tdb_insert(
            self._handle,
            collection.encode("utf-8"),
            json.dumps(document).encode("utf-8"),
            byref(id_buf)
        )
        _check_result(result)

        try:
            return id_buf.to_string()
        finally:
            self._lib.tdb_buffer_free(byref(id_buf))

    def get(self, collection: str, doc_id: str) -> Optional[Dict]:
        """Get a document by ID."""
        doc_buf = TdbBuffer()

        result = self._lib.tdb_get(
            self._handle,
            collection.encode("utf-8"),
            doc_id.encode("utf-8"),
            byref(doc_buf)
        )

        if result == -3:  # Not found
            return None
        _check_result(result)

        try:
            return doc_buf.to_json()
        finally:
            self._lib.tdb_buffer_free(byref(doc_buf))

    def update(self, collection: str, doc_id: str, updates: Dict) -> None:
        """Update a document by ID."""
        result = self._lib.tdb_update(
            self._handle,
            collection.encode("utf-8"),
            doc_id.encode("utf-8"),
            json.dumps(updates).encode("utf-8")
        )
        _check_result(result)

    def delete(self, collection: str, doc_id: str) -> None:
        """Delete a document by ID."""
        result = self._lib.tdb_delete(
            self._handle,
            collection.encode("utf-8"),
            doc_id.encode("utf-8")
        )
        _check_result(result)

    def query(self, collection: str, query: Dict) -> List[Dict]:
        """Execute a query on a collection."""
        results_buf = TdbBuffer()

        result = self._lib.tdb_query(
            self._handle,
            collection.encode("utf-8"),
            json.dumps(query).encode("utf-8"),
            byref(results_buf)
        )
        _check_result(result)

        try:
            return results_buf.to_json()
        finally:
            self._lib.tdb_buffer_free(byref(results_buf))

    def batch_insert(self, collection: str, documents: List[Dict]) -> int:
        """Insert multiple documents."""
        count = c_size_t()

        result = self._lib.tdb_batch_insert(
            self._handle,
            collection.encode("utf-8"),
            json.dumps(documents).encode("utf-8"),
            byref(count)
        )
        _check_result(result)

        return count.value

    def stats(self) -> Dict:
        """Get database statistics."""
        stats_buf = TdbBuffer()

        result = self._lib.tdb_stats(self._handle, byref(stats_buf))
        _check_result(result)

        try:
            return stats_buf.to_json()
        finally:
            self._lib.tdb_buffer_free(byref(stats_buf))

    @staticmethod
    def version() -> str:
        """Get TDB+ version."""
        lib = TdbCore._ensure_initialized()
        return lib.tdb_version().decode("utf-8")
