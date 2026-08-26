package story

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// The persistence contract and the in-memory implementation shipped with the library.

// Reader is the read surface of a store, shared by Store and Tx.
//
// Every method is self-contained: an implementation must not keep a read
// transaction, lock, or snapshot open once the call returns. Callers therefore
// never hold store resources across their own code, which is what makes it safe
// to write to the store from inside a read result — the trap a callback-scoped
// read transaction sets.
//
// The one place caller code does run inside an implementation is the fn passed
// to ScanRange and ScanPrefix. That fn belongs to this package and never
// re-enters the Store; see the scan documentation below.
//
// Every key and value a Reader hands out is owned by the implementation, which
// may return memory it does not copy — a page of a memory-mapped file, say. A
// caller must not mutate one, and must not retain one past its validity: for a
// value from Get, until the next call on the same Reader; for a key or value
// passed to a scan callback, until that callback returns. Anything needed for
// longer must be copied.
//
// Key ordering must be lexicographic over the raw byte slice — this is what
// bbolt, LevelDB, and most embedded KV stores provide out of the box.
// The scan methods rely on this ordering for prefix and time-index lookups.
type Reader interface {
	// Get returns the value for key, or nil if the key does not exist.
	Get(key []byte) ([]byte, error)

	// ScanRange calls fn for every key in [from, to) in ascending key order.
	// An empty to means "to the end of the key space". Iteration stops when fn
	// returns a non-nil error; that error is returned by ScanRange.
	//
	// An implementation may hold a read transaction, lock, or snapshot for the
	// duration of the scan, so fn must not call any method of the Store — not
	// another scan, not Get, and not Update. On bbolt a write that grows the
	// file blocks on every open read transaction, and on MemStore the RWMutex
	// is not reentrant; both deadlock. This package copies the keys it needs
	// out of the scan and reads the records after it returns.
	//
	// Calling back into the same Tx is allowed and is not the same thing: those
	// calls stay inside one transaction the implementation already holds.
	ScanRange(from, to []byte, fn func(key, val []byte) error) error

	// ScanPrefix calls fn for every key that begins with prefix, in ascending
	// key order. Iteration stops when fn returns a non-nil error. The same
	// no-reentry rule as ScanRange applies.
	//
	// Deleting or writing keys from fn is likewise not allowed: cursor
	// behaviour under concurrent mutation is undefined on most backends.
	// Collect what is to change, then apply it after the scan.
	ScanPrefix(prefix []byte, fn func(key, val []byte) error) error
}

// Store is the persistence interface used by Tracker.
//
// Implementations must be safe for concurrent use. Reads go straight through
// the embedded Reader; only writes are transactional, because only writes need
// to be atomic across several keys.
type Store interface {
	Reader

	// Update executes fn inside a read-write transaction.
	// Concurrent Update calls are serialised by the implementation.
	Update(fn func(tx Tx) error) error

	// Close flushes any buffered writes and releases resources.
	Close() error
}

// Tx is a read-write transaction provided to the Store.Update callback.
// Tx values must not be used outside the callback they are passed to, and
// nothing a Tx hands out survives the transaction: the Reader ownership rules
// apply, bounded additionally by the end of the callback.
type Tx interface {
	Reader

	// Put writes key → value. value must not be empty (use Delete to remove a key).
	Put(key, value []byte) error

	// Delete removes key. It is not an error if key does not exist.
	Delete(key []byte) error

	// DeletePrefix removes all keys that begin with prefix.
	DeletePrefix(prefix []byte) error
}

// MemStore is a thread-safe in-memory Store implementation suitable for tests and lightweight usage.
type MemStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemStore creates a new in-memory Store instance.
func NewMemStore() *MemStore {
	return &MemStore{data: make(map[string][]byte)}
}

func (s *MemStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return get(s.data, key)
}

func (s *MemStore) ScanRange(from, to []byte, fn func(key, val []byte) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return scanRange(s.data, from, to, fn)
}

func (s *MemStore) ScanPrefix(prefix []byte, fn func(key, val []byte) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return scanPrefix(s.data, prefix, fn)
}

func (s *MemStore) Update(fn func(Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(&memTx{store: s})
}

func (s *MemStore) Close() error { return nil }

// Compile-time interface checks.
var (
	_ Store = (*MemStore)(nil)
	_ Tx    = (*memTx)(nil)
)

// memTx is the write transaction. It runs under the store's write lock, so its
// read methods reach the map directly rather than taking the lock again.
type memTx struct{ store *MemStore }

func (t *memTx) Get(key []byte) ([]byte, error) { return get(t.store.data, key) }

func (t *memTx) ScanRange(from, to []byte, fn func(key, val []byte) error) error {
	return scanRange(t.store.data, from, to, fn)
}

func (t *memTx) ScanPrefix(prefix []byte, fn func(key, val []byte) error) error {
	return scanPrefix(t.store.data, prefix, fn)
}

func (t *memTx) Put(key, value []byte) error {
	if len(value) == 0 {
		return fmt.Errorf("memstore: value for key %q must not be empty", key)
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	t.store.data[string(key)] = cp
	return nil
}

func (t *memTx) Delete(key []byte) error {
	delete(t.store.data, string(key))
	return nil
}

func (t *memTx) DeletePrefix(prefix []byte) error {
	p := string(prefix)
	for k := range t.store.data {
		if strings.HasPrefix(k, p) {
			delete(t.store.data, k)
		}
	}
	return nil
}

// The read primitives, shared by MemStore and memTx. Each caller is responsible
// for holding whatever lock it needs before calling in.

func get(data map[string][]byte, key []byte) ([]byte, error) {
	v, ok := data[string(key)]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil
}

func scanRange(data map[string][]byte, from, to []byte, fn func(key, val []byte) error) error {
	fromS, toS := string(from), string(to)
	hasTo := len(to) > 0
	for _, k := range sortedKeys(data) {
		if k < fromS || (hasTo && k >= toS) {
			continue
		}
		if err := fn([]byte(k), append([]byte(nil), data[k]...)); err != nil {
			return err
		}
	}
	return nil
}

func scanPrefix(data map[string][]byte, prefix []byte, fn func(key, val []byte) error) error {
	p := string(prefix)
	for _, k := range sortedKeys(data) {
		if !strings.HasPrefix(k, p) {
			continue
		}
		if err := fn([]byte(k), append([]byte(nil), data[k]...)); err != nil {
			return err
		}
	}
	return nil
}

func sortedKeys(data map[string][]byte) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
