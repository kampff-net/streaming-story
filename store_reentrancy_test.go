package story

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kvsh.ch/streaming-story/internal/keys"
)

// Writing from inside a read is the shape that used to deadlock: a callback-
// scoped read transaction stayed open across caller code, so the write waited
// on a read that was waiting on the caller. It cost nothing to trip — an
// ordinary range over a public iterator with an Ingest in the loop body — and
// no caller could see it coming.
//
// Each subtest runs under the package test timeout; a regression hangs it
// rather than failing an assertion, which is the honest signal here.
func TestIterators_AllowWritesFromInsideTheLoop(t *testing.T) {
	ctx := context.Background()

	// seed puts a signal in the outlier bucket and returns its ID. Nothing is
	// promoted to a story until a batch runs, so both buckets get populated.
	seed := func(t *testing.T, tr *Tracker[string], x, y float32) uuid.UUID {
		t.Helper()
		id := uuid.New()
		_, err := tr.Ingest(ctx, Signal[string]{
			ID: id, At: time.Now(), Embeddings: []Embedding{{x, y}}, Data: "seed",
		})
		require.NoError(t, err)
		return id
	}

	// ingestInLoop is the write performed from inside an iterator body.
	ingestInLoop := func(t *testing.T, tr *Tracker[string]) {
		t.Helper()
		_, err := tr.Ingest(ctx, Signal[string]{
			ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{{0.5, 0.5}}, Data: "inner",
		})
		require.NoError(t, err)
	}

	newSeeded := func(t *testing.T) (*Tracker[string], uuid.UUID) {
		t.Helper()
		tr := newTestTracker(t)
		tr.dim.Store(2)
		for i := range 4 {
			seed(t, tr, 1, float32(i)/100)
		}
		tr.runBatch()
		// A story now exists; add a fresh outlier the batch has not seen.
		seed(t, tr, -1, 0.5)

		var storyID uuid.UUID
		for meta, err := range tr.Stories(StoryStateAny) {
			require.NoError(t, err)
			storyID = meta.ID
			break
		}
		require.NotEqual(t, uuid.Nil, storyID, "fixture must produce at least one story")
		return tr, storyID
	}

	t.Run("Stories", func(t *testing.T) {
		tr, _ := newSeeded(t)
		n := 0
		for range tr.Stories(StoryStateAny) {
			n++
			ingestInLoop(t, tr)
		}
		require.NotZero(t, n)
	})

	t.Run("Signals", func(t *testing.T) {
		tr, _ := newSeeded(t)
		n := 0
		for _, err := range tr.Signals() {
			require.NoError(t, err)
			n++
			ingestInLoop(t, tr)
		}
		require.NotZero(t, n)
	})

	t.Run("Outliers", func(t *testing.T) {
		tr, _ := newSeeded(t)
		n := 0
		for _, err := range tr.Outliers() {
			require.NoError(t, err)
			n++
			ingestInLoop(t, tr)
		}
		require.NotZero(t, n)
	})

	t.Run("SignalsOf", func(t *testing.T) {
		tr, storyID := newSeeded(t)
		n := 0
		for _, err := range tr.SignalsOf(storyID) {
			require.NoError(t, err)
			n++
			ingestInLoop(t, tr)
		}
		require.NotZero(t, n)
	})

	t.Run("FacetsOfStory", func(t *testing.T) {
		tr, storyID := newSeeded(t)
		n := 0
		for _, err := range tr.FacetsOfStory(storyID) {
			require.NoError(t, err)
			n++
			ingestInLoop(t, tr)
		}
		require.NotZero(t, n)
	})
}

// A store read must not be open while the caller decides to stop, either: an
// early break is the common case for a large iterator.
func TestIterators_AllowWritesAfterEarlyBreak(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)
	for i := range 4 {
		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{{1, float32(i) / 100}},
		})
		require.NoError(t, err)
	}

	for range tr.Outliers() {
		break
	}

	_, err := tr.Ingest(context.Background(), Signal[string]{
		ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{{0.5, 0.5}},
	})
	require.NoError(t, err)
}

// Stories used to drop store and decode failures on the floor: its body ended
// in `_ = Store.View(...)`, so a caller ranging over it could not tell an empty
// store from a broken one.
func TestStories_ReportsDecodeFailures(t *testing.T) {
	tr := newTestTracker(t)
	tr.dim.Store(2)

	for i := range 3 {
		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID: uuid.New(), At: time.Now(), Embeddings: []Embedding{{1, float32(i) / 100}},
		})
		require.NoError(t, err)
	}
	tr.runBatch()

	var storyID uuid.UUID
	for meta, err := range tr.Stories(StoryStateAny) {
		require.NoError(t, err)
		storyID = meta.ID
		break
	}
	require.NotEqual(t, uuid.Nil, storyID, "fixture must produce a story")

	// Corrupt the record in place; the key still parses as story metadata.
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return tx.Put(keys.StoryMeta(storyID), []byte("not cbor"))
	}))

	var errs []error
	var metas []StoryMeta
	for meta, err := range tr.Stories(StoryStateAny) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		metas = append(metas, meta)
	}

	require.Len(t, errs, 1, "the corrupt record must surface as an error")
	require.ErrorContains(t, errs[0], storyID.String())
	for _, m := range metas {
		assert.NotEqual(t, storyID, m.ID, "the corrupt record must not yield a half-built StoryMeta")
	}
}
