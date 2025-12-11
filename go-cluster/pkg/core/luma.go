// Package core provides Go bindings to the Rust TDB+ storage engine via CGO.
package core

/*
#cgo LDFLAGS: -L"${SRCDIR}/../../../rust-core/target/release" -lluma_core -ldl -lm
#include <stdlib.h>
#include <stdint.h>

typedef int LumaResult;
typedef uint64_t LumaHandle;

typedef struct {
    uint8_t* data;
    size_t len;
    size_t capacity;
} LumaBuffer;

// Database lifecycle
extern LumaResult luma_open(const char* path, const char* config_json, LumaHandle* handle_out);
extern LumaResult luma_close(LumaHandle handle);

// Collection operations
extern LumaResult luma_create_collection(LumaHandle handle, const char* name);
extern LumaResult luma_drop_collection(LumaHandle handle, const char* name);

// Document operations
extern LumaResult luma_insert(LumaHandle handle, const char* collection, const char* doc_json, LumaBuffer* id_out);
extern LumaResult luma_insert_mp(LumaHandle handle, const char* collection, const uint8_t* input_data, size_t input_len, LumaBuffer* id_out);
extern LumaResult luma_get(LumaHandle handle, const char* collection, const char* id, LumaBuffer* doc_out);
extern LumaResult luma_update(LumaHandle handle, const char* collection, const char* id, const char* updates);
extern LumaResult luma_delete(LumaHandle handle, const char* collection, const char* id);

// Query operations
extern LumaResult luma_query(LumaHandle handle, const char* collection, const char* query, LumaBuffer* results_out);
extern LumaResult luma_batch_insert(LumaHandle handle, const char* collection, const char* docs, size_t* count);
extern LumaResult luma_search_vector(LumaHandle handle, const char* vector_json, size_t k, LumaBuffer* results_out);

// Memory management
extern void luma_buffer_free(LumaBuffer* buffer);
extern const uint8_t* luma_buffer_data(const LumaBuffer* buffer);
extern size_t luma_buffer_len(const LumaBuffer* buffer);

// Info
extern LumaResult luma_stats(LumaHandle handle, LumaBuffer* stats_out);
extern const char* luma_version();
*/
import "C"

import (
	"encoding/json"
	"errors"
	"sync"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"
)

// Error codes
var (
	ErrInvalidHandle   = errors.New("invalid database handle")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrNotFound        = errors.New("document not found")
	ErrAlreadyExists   = errors.New("document already exists")
	ErrIO              = errors.New("I/O error")
	ErrCorruption      = errors.New("data corruption")
	ErrFull            = errors.New("storage full")
	ErrInternal        = errors.New("internal error")
)

func resultToError(result C.LumaResult) error {
	switch result {
	case 0:
		return nil
	case -1:
		return ErrInvalidHandle
	case -2:
		return ErrInvalidArgument
	case -3:
		return ErrNotFound
	case -4:
		return ErrAlreadyExists
	case -5:
		return ErrIO
	case -6:
		return ErrCorruption
	case -7:
		return ErrFull
	default:
		return ErrInternal
	}
}

// Database represents a connection to the TDB+ storage engine
type Database struct {
	handle C.LumaHandle
	mu     sync.RWMutex
	closed bool
}

// Open opens a database at the given path
func Open(path string, config map[string]interface{}) (*Database, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cConfig *C.char
	if config != nil {
		configJSON, err := json.Marshal(config)
		if err != nil {
			return nil, err
		}
		cConfig = C.CString(string(configJSON))
		defer C.free(unsafe.Pointer(cConfig))
	}

	var handle C.LumaHandle
	result := C.luma_open(cPath, cConfig, &handle)
	if err := resultToError(result); err != nil {
		return nil, err
	}

	return &Database{handle: handle}, nil
}

// Close closes the database
func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	result := C.luma_close(db.handle)
	db.closed = true
	return resultToError(result)
}

// CreateCollection creates a new collection
func (db *Database) CreateCollection(name string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.luma_create_collection(db.handle, cName)
	return resultToError(result)
}

// DropCollection removes a collection
func (db *Database) DropCollection(name string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.luma_drop_collection(db.handle, cName)
	return resultToError(result)
}

// Insert inserts a document into a collection
func (db *Database) Insert(collection string, doc interface{}) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	docJSON, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}

	cCollection := C.CString(collection)
	defer C.free(unsafe.Pointer(cCollection))

	cDoc := C.CString(string(docJSON))
	defer C.free(unsafe.Pointer(cDoc))

	var idBuf C.LumaBuffer
	result := C.luma_insert(db.handle, cCollection, cDoc, &idBuf)
	if err := resultToError(result); err != nil {
		return "", err
	}
	defer C.luma_buffer_free(&idBuf)

	id := C.GoStringN((*C.char)(unsafe.Pointer(idBuf.data)), C.int(idBuf.len))
	return id, nil
}

// InsertMP inserts a document using MessagePack encoding (faster than JSON)
func (db *Database) InsertMP(collection string, doc interface{}) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	data, err := msgpack.Marshal(doc)
	if err != nil {
		return "", err
	}

	cCollection := C.CString(collection)
	defer C.free(unsafe.Pointer(cCollection))

	var idBuf C.LumaBuffer
	result := C.luma_insert_mp(
		db.handle,
		cCollection,
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		&idBuf,
	)
	if err := resultToError(result); err != nil {
		return "", err
	}
	defer C.luma_buffer_free(&idBuf)

	id := C.GoStringN((*C.char)(unsafe.Pointer(idBuf.data)), C.int(idBuf.len))
	return id, nil
}

// Get retrieves a document by ID
func (db *Database) Get(collection, id string) (map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	cCollection := C.CString(collection)
	defer C.free(unsafe.Pointer(cCollection))

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))

	var docBuf C.LumaBuffer
	result := C.luma_get(db.handle, cCollection, cID, &docBuf)
	if err := resultToError(result); err != nil {
		return nil, err
	}
	defer C.luma_buffer_free(&docBuf)

	docJSON := C.GoBytes(unsafe.Pointer(docBuf.data), C.int(docBuf.len))
	var doc map[string]interface{}
	if err := json.Unmarshal(docJSON, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// Update updates a document by ID
func (db *Database) Update(collection, id string, updates interface{}) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	updatesJSON, err := json.Marshal(updates)
	if err != nil {
		return err
	}

	cCollection := C.CString(collection)
	defer C.free(unsafe.Pointer(cCollection))

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))

	cUpdates := C.CString(string(updatesJSON))
	defer C.free(unsafe.Pointer(cUpdates))

	result := C.luma_update(db.handle, cCollection, cID, cUpdates)
	return resultToError(result)
}

// Delete removes a document by ID
func (db *Database) Delete(collection, id string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	cCollection := C.CString(collection)
	defer C.free(unsafe.Pointer(cCollection))

	cID := C.CString(id)
	defer C.free(unsafe.Pointer(cID))

	result := C.luma_delete(db.handle, cCollection, cID)
	return resultToError(result)
}

// Query executes a query on a collection
func (db *Database) Query(collection string, query interface{}) ([]map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	cCollection := C.CString(collection)
	defer C.free(unsafe.Pointer(cCollection))

	cQuery := C.CString(string(queryJSON))
	defer C.free(unsafe.Pointer(cQuery))

	var resultsBuf C.LumaBuffer
	result := C.luma_query(db.handle, cCollection, cQuery, &resultsBuf)
	if err := resultToError(result); err != nil {
		return nil, err
	}
	defer C.luma_buffer_free(&resultsBuf)

	resultsJSON := C.GoBytes(unsafe.Pointer(resultsBuf.data), C.int(resultsBuf.len))
	var results []map[string]interface{}
	if err := json.Unmarshal(resultsJSON, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// VectorSearch searches for similar vectors
func (db *Database) VectorSearch(vector []float32, k int) ([]map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	vectorJSON, err := json.Marshal(vector)
	if err != nil {
		return nil, err
	}

	cVector := C.CString(string(vectorJSON))
	defer C.free(unsafe.Pointer(cVector))

	var resultsBuf C.LumaBuffer
	result := C.luma_search_vector(db.handle, cVector, C.size_t(k), &resultsBuf)
	if err := resultToError(result); err != nil {
		return nil, err
	}
	defer C.luma_buffer_free(&resultsBuf)

	resultsJSON := C.GoBytes(unsafe.Pointer(resultsBuf.data), C.int(resultsBuf.len))
	var results []map[string]interface{}
	if err := json.Unmarshal(resultsJSON, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// BatchInsert inserts multiple documents
func (db *Database) BatchInsert(collection string, docs []interface{}) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	docsJSON, err := json.Marshal(docs)
	if err != nil {
		return 0, err
	}

	cCollection := C.CString(collection)
	defer C.free(unsafe.Pointer(cCollection))

	cDocs := C.CString(string(docsJSON))
	defer C.free(unsafe.Pointer(cDocs))

	var count C.size_t
	result := C.luma_batch_insert(db.handle, cCollection, cDocs, &count)
	if err := resultToError(result); err != nil {
		return 0, err
	}
	return int(count), nil
}

// Stats returns database statistics
func (db *Database) Stats() (map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var statsBuf C.LumaBuffer
	result := C.luma_stats(db.handle, &statsBuf)
	if err := resultToError(result); err != nil {
		return nil, err
	}
	defer C.luma_buffer_free(&statsBuf)

	statsJSON := C.GoBytes(unsafe.Pointer(statsBuf.data), C.int(statsBuf.len))
	var stats map[string]interface{}
	if err := json.Unmarshal(statsJSON, &stats); err != nil {
		return nil, err
	}
	return stats, nil
}

// Version returns the TDB+ version
func Version() string {
	return C.GoString(C.luma_version())
}
