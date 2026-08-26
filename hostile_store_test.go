package story

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kvsh.ch/streaming-story/internal/dist"
)

// hostileStore is a test Store wrapper that allocates fresh byte slices on
// reads, records every slice it returns, and clobbers them with garbage bytes
// (0xCC) the moment they stop being valid under the Reader contract: at the end
// of the enclosing Update for a transactional read, and at the next store call
// for a standalone one. Any code that retained a store slice reads 0xCC.
type hostileStore struct {
	mu   sync.Mutex
	impl *MemStore

	// stale holds the buffers handed out by the previous standalone read; they
	// are poisoned when the next call arrives.
	stale [][]byte
}

func newHostileStore() *hostileStore {
	return &hostileStore{impl: newMemStore()}
}

func (h *hostileStore) Close() error {
	return h.impl.Close()
}

func poison(bufs [][]byte) {
	for _, b := range bufs {
		for i := range b {
			b[i] = 0xCC
		}
	}
}

// tracker returns a copying tracker plus the slice recording what it handed out.
func tracker() (func([]byte) []byte, *[][]byte) {
	var returned [][]byte
	return func(b []byte) []byte {
		if b == nil {
			return nil
		}
		cp := bytes.Clone(b)
		returned = append(returned, cp)
		return cp
	}, &returned
}

// standalone runs a single non-transactional read. Buffers from the previous
// standalone read are poisoned on entry, which is exactly when the Reader
// contract says they expire.
func (h *hostileStore) standalone(fn func(tx Tx, track func([]byte) []byte) error) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	poison(h.stale)
	track, returned := tracker()
	err := fn(&memTx{store: h.impl}, track)
	h.stale = *returned
	return err
}

func (h *hostileStore) Get(key []byte) ([]byte, error) {
	var out []byte
	err := h.standalone(func(tx Tx, track func([]byte) []byte) error {
		b, err := tx.Get(key)
		if err != nil {
			return err
		}
		out = track(b)
		return nil
	})
	return out, err
}

// Scans poison on return: a scan hands its slices to the callback, and the
// contract ends them when that callback returns.
func (h *hostileStore) ScanRange(from, to []byte, fn func(key, val []byte) error) error {
	return h.scan(fn, func(tx Tx, wrapped func(key, val []byte) error) error {
		return tx.ScanRange(from, to, wrapped)
	})
}

func (h *hostileStore) ScanPrefix(prefix []byte, fn func(key, val []byte) error) error {
	return h.scan(fn, func(tx Tx, wrapped func(key, val []byte) error) error {
		return tx.ScanPrefix(prefix, wrapped)
	})
}

func (h *hostileStore) scan(fn func(key, val []byte) error, run func(Tx, func(key, val []byte) error) error) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	poison(h.stale)
	h.stale = nil

	err := run(&memTx{store: h.impl}, func(key, val []byte) error {
		track, returned := tracker()
		ferr := fn(track(key), track(val))
		poison(*returned)
		return ferr
	})
	return err
}

func (h *hostileStore) Update(fn func(tx Tx) error) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	poison(h.stale)
	h.stale = nil

	track, returned := tracker()
	err := fn(&hostileTx{tx: &memTx{store: h.impl}, trackBuf: track})
	poison(*returned)
	return err
}

type hostileTx struct {
	tx       Tx
	trackBuf func([]byte) []byte
}

func (hx *hostileTx) Get(key []byte) ([]byte, error) {
	b, err := hx.tx.Get(key)
	if err != nil {
		return nil, err
	}
	return hx.trackBuf(b), nil
}

func (hx *hostileTx) Put(key, value []byte) error {
	// MemStore already clones keys and values internally, but ensure Put doesn't fail
	return hx.tx.Put(key, value)
}

func (hx *hostileTx) Delete(key []byte) error {
	return hx.tx.Delete(key)
}

func (hx *hostileTx) DeletePrefix(prefix []byte) error {
	return hx.tx.DeletePrefix(prefix)
}

func (hx *hostileTx) ScanRange(from, to []byte, fn func(key, val []byte) error) error {
	return hx.tx.ScanRange(from, to, func(key, val []byte) error {
		return fn(hx.trackBuf(key), hx.trackBuf(val))
	})
}

func (hx *hostileTx) ScanPrefix(prefix []byte, fn func(key, val []byte) error) error {
	return hx.tx.ScanPrefix(prefix, func(key, val []byte) error {
		return fn(hx.trackBuf(key), hx.trackBuf(val))
	})
}

var (
	_ Store = (*hostileStore)(nil)
	_ Tx    = (*hostileTx)(nil)
)

func TestHostileStore_IngestAndBatch(t *testing.T) {
	store := newHostileStore()
	tr, err := NewTracker[string](Config[string]{
		Store:           store,
		BatchInterval:   time.Hour,
		MinStorySize:    2,
		AssignThreshold: 0.3,
		MergeThreshold:  0.2,
		SplitThreshold:  0.4,
		MeanRemoval:     0.9,
	})
	require.NoError(t, err)
	defer tr.Close()

	ctx := context.Background()
	now := time.Now()

	// Ingest several signals
	sig1 := Signal[string]{
		ID:         uuid.New(),
		At:         now,
		Embeddings: []Embedding{[]float32{1, 0, 0}},
		Data:       "doc1",
	}
	sig2 := Signal[string]{
		ID:         uuid.New(),
		At:         now,
		Embeddings: []Embedding{[]float32{0.99, 0.05, 0}},
		Data:       "doc2",
	}
	sig3 := Signal[string]{
		ID:         uuid.New(),
		At:         now,
		Embeddings: []Embedding{[]float32{0, 1, 0}},
		Data:       "doc3",
	}

	_, err = tr.Ingest(ctx, sig1)
	require.NoError(t, err)
	_, err = tr.Ingest(ctx, sig2)
	require.NoError(t, err)
	_, err = tr.Ingest(ctx, sig3)
	require.NoError(t, err)

	// Run batch over hostile store
	tr.runBatch()

	// Query stories and signals
	var stories []StoryMeta
	for s, err := range tr.Stories(StoryStateAny) {
		require.NoError(t, err)
		stories = append(stories, s)
	}
	assert.NotEmpty(t, stories)

	for _, s := range stories {
		sigCount := 0
		for sig, err := range tr.SignalsOf(s.ID) {
			require.NoError(t, err)
			assert.NotEmpty(t, sig.Data)
			sigCount++
		}
		assert.Positive(t, sigCount)
	}
}

func TestUnitVectorInvariants(t *testing.T) {
	tr := newTestTracker(t)
	now := time.Now()

	// Ingest non-unit vectors and verify they become unit length in store
	rawVec := []float32{3.0, 4.0}
	sig := Signal[string]{
		ID:         uuid.New(),
		At:         now,
		Embeddings: []Embedding{rawVec},
		Data:       "unscaled",
	}
	_, err := tr.Ingest(context.Background(), sig)
	require.NoError(t, err)

	stored, err := tr.Signal(sig.ID)
	require.NoError(t, err)
	require.Len(t, stored.Embeddings, 1)

	n := dist.Norm(stored.Embeddings[0])
	assert.InDelta(t, 1.0, n, 1e-5, "stored embedding must have unit norm")
	assert.InDelta(t, 0.6, stored.Embeddings[0][0], 1e-5)
	assert.InDelta(t, 0.8, stored.Embeddings[0][1], 1e-5)
}
