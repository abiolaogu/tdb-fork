// Package core provides Go bindings to the Rust TDB+ storage engine via CGO.
package core

/*
#cgo LDFLAGS: -L${SRCDIR}/../../rust-core/target/release -ltdb_core -ldl -lm
#cgo CFLAGS: -I${SRCDIR}/../../rust-core/include

#include <stdlib.h>
#include <stdint.h>

typedef int TdbResult;
typedef uint64_t TdbHandle;

typedef struct {
    uint8_t* data;
    size_t len;
    size_t capacity;
} TdbBuffer;

// Database lifecycle
extern TdbResult tdb_open(const char* path, const char* config_json, TdbHandle* handle_out);
extern TdbResult tdb_close(TdbHandle handle);

// Collection operations
extern TdbResult tdb_create_collection(TdbHandle handle, const char* name);
extern TdbResult tdb_drop_collection(TdbHandle handle, const char* name);

// Document operations
extern TdbResult tdb_insert(TdbHandle handle, const char* collection, const char* doc_json, TdbBuffer* id_out);
extern TdbResult tdb_get(TdbHandle handle, const char* collection, const char* id, TdbBuffer* doc_out);
extern TdbResult tdb_update(TdbHandle handle, const char* collection, const char* id, const char* updates);
extern TdbResult tdb_delete(TdbHandle handle, const char* collection, const char* id);

// Query operations
extern TdbResult tdb_query(TdbHandle handle, const char* collection, const char* query, TdbBuffer* results_out);
extern TdbResult tdb_batch_insert(TdbHandle handle, const char* collection, const char* docs, size_t* count);

// Memory management
extern void tdb_buffer_free(TdbBuffer* buffer);
extern const uint8_t* tdb_buffer_data(const TdbBuffer* buffer);
extern size_t tdb_buffer_len(const TdbBuffer* buffer);

// Info
extern TdbResult tdb_stats(TdbHandle handle, TdbBuffer* stats_out);
extern const char* tdb_version();
*/
import "C"

import (
	"encoding/json"
	"errors"
	"sync"
	"unsafe"
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

func resultToError(result C.TdbResult) error {
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
	handle C.TdbHandle
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

	var handle C.TdbHandle
	result := C.tdb_open(cPath, cConfig, &handle)
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

	result := C.tdb_close(db.handle)
	db.closed = true
	return resultToError(result)
}

// CreateCollection creates a new collection
func (db *Database) CreateCollection(name string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.tdb_create_collection(db.handle, cName)
	return resultToError(result)
}

// DropCollection removes a collection
func (db *Database) DropCollection(name string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.tdb_drop_collection(db.handle, cName)
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

	var idBuf C.TdbBuffer
	result := C.tdb_insert(db.handle, cCollection, cDoc, &idBuf)
	if err := resultToError(result); err != nil {
		return "", err
	}
	defer C.tdb_buffer_free(&idBuf)

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

	var docBuf C.TdbBuffer
	result := C.tdb_get(db.handle, cCollection, cID, &docBuf)
	if err := resultToError(result); err != nil {
		return nil, err
	}
	defer C.tdb_buffer_free(&docBuf)

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

	result := C.tdb_update(db.handle, cCollection, cID, cUpdates)
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

	result := C.tdb_delete(db.handle, cCollection, cID)
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

	var resultsBuf C.TdbBuffer
	result := C.tdb_query(db.handle, cCollection, cQuery, &resultsBuf)
	if err := resultToError(result); err != nil {
		return nil, err
	}
	defer C.tdb_buffer_free(&resultsBuf)

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
	result := C.tdb_batch_insert(db.handle, cCollection, cDocs, &count)
	if err := resultToError(result); err != nil {
		return 0, err
	}
	return int(count), nil
}

// Stats returns database statistics
func (db *Database) Stats() (map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var statsBuf C.TdbBuffer
	result := C.tdb_stats(db.handle, &statsBuf)
	if err := resultToError(result); err != nil {
		return nil, err
	}
	defer C.tdb_buffer_free(&statsBuf)

	statsJSON := C.GoBytes(unsafe.Pointer(statsBuf.data), C.int(statsBuf.len))
	var stats map[string]interface{}
	if err := json.Unmarshal(statsJSON, &stats); err != nil {
		return nil, err
	}
	return stats, nil
}

// Version returns the TDB+ version
func Version() string {
	return C.GoString(C.tdb_version())
}
