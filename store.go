package story

import (
	"fmt"
	"sort"
	"sync"
)

// The persistence and encoding contracts, and the implementations shipped
// for them: MemStore for tests and small deployments, CBORCodec as the
// default payload encoding.

// Store is the persistence interface used by Tracker.
//
// Implementations must be safe for concurrent use; the Tracker serialises
// writes internally but may call View concurrently with other View calls.
//
// Key ordering must be lexicographic over the raw byte slice — this is what
// bbolt, LevelDB, and most embedded KV stores provide out of the box.
// Range scan methods rely on this ordering for efficient prefix and time-index lookups.
type Store interface {
	// View executes fn inside a read-only transaction.
	View(fn func(tx Tx) error) error

	// Update executes fn inside a read-write transaction.
	// Concurrent Update calls are serialised by the implementation.
	Update(fn func(tx Tx) error) error

	// Close flushes any buffered writes and releases resources.
	Close() error
}

// Tx is a single key-value transaction provided to Store.View and Store.Update callbacks.
// Tx values must not be used outside the callback they are passed to.
// All byte slices passed to or returned from Tx callbacks (keys and values in Get,
// ScanPrefix, ScanRange) are owned by the store only for the duration of the callback/transaction.
// Callers must not mutate them and must not retain references past transaction completion.
type Tx interface {
	// Get returns the value for key, or nil if the key does not exist.
	// The returned slice is only valid for the lifetime of the transaction.
	Get(key []byte) ([]byte, error)

	// Put writes key → value. value must not be empty (use Delete to remove a key).
	Put(key, value []byte) error

	// Delete removes key. It is not an error if key does not exist.
	Delete(key []byte) error

	// DeletePrefix removes all keys that begin with prefix.
	DeletePrefix(prefix []byte) error

	// ScanRange calls fn for every key in [from, to) in ascending key order.
	// Iteration stops when fn returns a non-nil error; that error is returned
	// by ScanRange.
	ScanRange(from, to []byte, fn func(key, val []byte) error) error

	// ScanPrefix calls fn for every key that begins with prefix, in ascending
	// key order. Iteration stops when fn returns a non-nil error.
	ScanPrefix(prefix []byte, fn func(key, val []byte) error) error
}

// Codec encodes and decodes Signal[T] values for persistence.
type Codec[T any] interface {
	Encode(sig Signal[T]) ([]byte, error)
	Decode(b []byte) (Signal[T], error)
}

// CBORCodec encodes signals with canonical CBOR (github.com/fxamacker/cbor/v2)
// using integer keys. It is the default choice.
type CBORCodec[T any] struct{}

// Compile-time interface check.
var _ Codec[struct{}] = CBORCodec[struct{}]{}

// Encode implements Codec.
func (CBORCodec[T]) Encode(sig Signal[T]) ([]byte, error) {
	return cborEncMode.Marshal(sig)
}

// Decode implements Codec.
func (CBORCodec[T]) Decode(b []byte) (Signal[T], error) {
	var sig Signal[T]
	if err := cborDecMode.Unmarshal(b, &sig); err != nil {
		return Signal[T]{}, err
	}
	return sig, nil
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

func (s *MemStore) View(fn func(Tx) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fn(&memTx{store: s})
}

func (s *MemStore) Update(fn func(Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(&memTx{store: s})
}

func (s *MemStore) Close() error { return nil }

// Compile-time interface check.
var _ Store = (*MemStore)(nil)

type memTx struct{ store *MemStore }

var _ Tx = (*memTx)(nil)

func (t *memTx) Get(key []byte) ([]byte, error) {
	v, ok := t.store.data[string(key)]
	if !ok {
		return nil, nil
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
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
		if len(k) >= len(p) && k[:len(p)] == p {
			delete(t.store.data, k)
		}
	}
	return nil
}

func (t *memTx) ScanRange(from, to []byte, fn func(key, val []byte) error) error {
	fromS, toS := string(from), string(to)
	hasTo := len(to) > 0
	for _, k := range t.sortedKeys() {
		if k < fromS || (hasTo && k >= toS) {
			continue
		}
		v := t.store.data[k]
		cp := make([]byte, len(v))
		copy(cp, v)
		if err := fn([]byte(k), cp); err != nil {
			return err
		}
	}
	return nil
}

func (t *memTx) ScanPrefix(prefix []byte, fn func(key, val []byte) error) error {
	p := string(prefix)
	for _, k := range t.sortedKeys() {
		if len(k) < len(p) || k[:len(p)] != p {
			continue
		}
		v := t.store.data[k]
		cp := make([]byte, len(v))
		copy(cp, v)
		if err := fn([]byte(k), cp); err != nil {
			return err
		}
	}
	return nil
}

func (t *memTx) sortedKeys() []string {
	keys := make([]string, 0, len(t.store.data))
	for k := range t.store.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
