package story

import (
	"sort"
	"sync"
)

// MemStore is an thread-safe in-memory Store implementation suitable for tests and lightweight usage.
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
