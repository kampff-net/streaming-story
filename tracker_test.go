package story

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.kvsh.ch/streaming-story/internal/keys"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestTracker creates a Tracker backed by an in-memory store.
// BatchInterval is set to 1 hour so the ticker never fires during a unit test.
func newTestTracker(t *testing.T) *Tracker[string] {
	t.Helper()
	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		Codec:         JSONCodec[string]{},
		BatchInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func TestNewTracker(t *testing.T) {
	t.Run("valid_config", func(t *testing.T) {
		tr := newTestTracker(t)
		assert.NotNil(t, tr)
	})

	t.Run("nil_store_returns_error", func(t *testing.T) {
		_, err := NewTracker[string](Config[string]{Codec: JSONCodec[string]{}})
		require.Error(t, err)
	})

	t.Run("nil_codec_returns_error", func(t *testing.T) {
		_, err := NewTracker[string](Config[string]{Store: newMemStore()})
		require.Error(t, err)
	})

	t.Run("loads_persisted_calib_state", func(t *testing.T) {
		ms := newMemStore()
		state := calibState{SigmaGlobal: 0.42, Dim: 64}
		b, err := json.Marshal(state)
		require.NoError(t, err)
		require.NoError(t, ms.Update(func(tx Tx) error {
			return tx.Put(keys.CalibState(), b)
		}))

		tr, err := NewTracker[string](Config[string]{
			Store:         ms,
			Codec:         JSONCodec[string]{},
			BatchInterval: time.Hour,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })

		assert.EqualValues(t, 64, tr.dim.Load())
		assert.Equal(t, 0.42, tr.sigmaGlobal)
	})
}

func TestTracker_Subscribe(t *testing.T) {
	t.Run("returns_channel_with_configured_buffer", func(t *testing.T) {
		tr := newTestTracker(t)
		ch := tr.Subscribe()
		assert.Equal(t, tr.cfg.EventBufferSize, cap(ch))
	})

	t.Run("each_call_returns_independent_channel", func(t *testing.T) {
		tr := newTestTracker(t)
		ch1 := tr.Subscribe()
		ch2 := tr.Subscribe()
		assert.NotSame(t, &ch1, &ch2)
		assert.Equal(t, 2, len(tr.subs))
	})
}

func TestTracker_Close(t *testing.T) {
	t.Run("returns_without_blocking", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{
			Store:         newMemStore(),
			Codec:         JSONCodec[string]{},
			BatchInterval: time.Hour,
		})
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() { done <- tr.Close() }()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("Close blocked")
		}
	})

	t.Run("closes_subscriber_channels", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{
			Store:         newMemStore(),
			Codec:         JSONCodec[string]{},
			BatchInterval: time.Hour,
		})
		require.NoError(t, err)

		ch := tr.Subscribe()
		require.NoError(t, tr.Close())

		_, ok := <-ch
		assert.False(t, ok, "subscriber channel should be closed after Close()")
	})
}

func TestTracker_emit(t *testing.T) {
	t.Run("delivers_event_to_subscriber", func(t *testing.T) {
		tr := newTestTracker(t)
		ch := tr.Subscribe()

		tr.emit(StoryEvent[string]{Kind: EventStoryCreated, At: time.Now()})

		select {
		case got := <-ch:
			assert.Equal(t, EventStoryCreated, got.Kind)
		case <-time.After(time.Second):
			t.Fatal("event not received within timeout")
		}
	})

	t.Run("delivers_to_all_subscribers", func(t *testing.T) {
		tr := newTestTracker(t)
		ch1 := tr.Subscribe()
		ch2 := tr.Subscribe()

		tr.emit(StoryEvent[string]{Kind: EventStoryCreated})

		assert.Equal(t, 1, len(ch1))
		assert.Equal(t, 1, len(ch2))
	})

	t.Run("does_not_block_on_full_channel", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{
			Store:           newMemStore(),
			Codec:           JSONCodec[string]{},
			BatchInterval:   time.Hour,
			EventBufferSize: 1,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })

		ch := tr.Subscribe()
		// Subscribe returns a receive-only channel; access the internal
		// bidirectional channel to pre-fill it.
		tr.subMu.Lock()
		tr.subs[0] <- StoryEvent[string]{Kind: EventDraftAssigned}
		tr.subMu.Unlock()

		done := make(chan struct{})
		go func() {
			tr.emit(StoryEvent[string]{Kind: EventStoryCreated})
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("emit blocked on a full subscriber channel")
		}
		assert.Equal(t, 1, len(ch), "channel should be unchanged: overflow event was dropped")
	})

	t.Run("no_op_after_close", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{
			Store:         newMemStore(),
			Codec:         JSONCodec[string]{},
			BatchInterval: time.Hour,
		})
		require.NoError(t, err)
		require.NoError(t, tr.Close())

		assert.NotPanics(t, func() {
			tr.emit(StoryEvent[string]{Kind: EventStoryCreated})
		})
	})
}

func TestTracker_loadCalibState(t *testing.T) {
	t.Run("empty_store_is_noop", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{Store: newMemStore()}}
		require.NoError(t, tr.loadCalibState())
		assert.Zero(t, tr.dim.Load())
		assert.Zero(t, tr.sigmaGlobal)
	})

	t.Run("loads_dim_and_sigma_from_store", func(t *testing.T) {
		ms := newMemStore()
		b, _ := json.Marshal(calibState{SigmaGlobal: 1.23, Dim: 128})
		require.NoError(t, ms.Update(func(tx Tx) error {
			return tx.Put(keys.CalibState(), b)
		}))

		tr := &Tracker[string]{cfg: Config[string]{Store: ms}}
		require.NoError(t, tr.loadCalibState())

		assert.EqualValues(t, 128, tr.dim.Load())
		assert.Equal(t, 1.23, tr.sigmaGlobal)
	})
}

func TestTracker_saveCalibState(t *testing.T) {
	ms := newMemStore()
	tr := &Tracker[string]{cfg: Config[string]{Store: ms}}
	tr.dim.Store(32)
	tr.sigmaGlobal = 0.99
	tr.lastBatch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	require.NoError(t, ms.Update(func(tx Tx) error {
		return tr.saveCalibState(tx)
	}))

	var got calibState
	require.NoError(t, ms.View(func(tx Tx) error {
		b, err := tx.Get(keys.CalibState())
		if err != nil {
			return err
		}
		return json.Unmarshal(b, &got)
	}))

	assert.Equal(t, 32, got.Dim)
	assert.Equal(t, 0.99, got.SigmaGlobal)
	assert.True(t, tr.lastBatch.Equal(got.LastBatchAt))
}

func TestTracker_readStoryMeta(t *testing.T) {
	t.Run("missing_key_returns_ErrNotFound", func(t *testing.T) {
		tr := &Tracker[string]{cfg: Config[string]{Store: newMemStore()}}
		err := tr.cfg.Store.View(func(tx Tx) error {
			_, e := tr.readStoryMeta(tx, uuid.New())
			return e
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotFound))
	})

	t.Run("returns_correct_metadata", func(t *testing.T) {
		ms := newMemStore()
		id := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
		rec := storyRecord{
			State:        StoryStateActive,
			Centroid:     []float32{1.0, 2.0, 3.0},
			Radius:       0.5,
			CreatedAt:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			LastSignalAt: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		}
		b, _ := json.Marshal(rec)
		require.NoError(t, ms.Update(func(tx Tx) error {
			return tx.Put(keys.StoryMeta(id), b)
		}))

		tr := &Tracker[string]{cfg: Config[string]{Store: ms}}
		var meta StoryMeta
		require.NoError(t, ms.View(func(tx Tx) error {
			var e error
			meta, e = tr.readStoryMeta(tx, id)
			return e
		}))

		assert.Equal(t, id, meta.ID)
		assert.Equal(t, StoryStateActive, meta.State)
		assert.Equal(t, []float32{1.0, 2.0, 3.0}, meta.Centroid)
		assert.Equal(t, 0.5, meta.Radius)
		assert.True(t, rec.CreatedAt.Equal(meta.CreatedAt))
		assert.True(t, rec.LastSignalAt.Equal(meta.LastSignalAt))
	})
}

func TestTracker_Story(t *testing.T) {
	t.Run("unknown_id_returns_ErrNotFound", func(t *testing.T) {
		tr := newTestTracker(t)
		_, err := tr.Story(uuid.New())
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotFound))
	})

	t.Run("returns_story_metadata", func(t *testing.T) {
		tr := newTestTracker(t)
		id := uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
		rec := storyRecord{
			State:        StoryStateDormant,
			Centroid:     []float32{1.0},
			CreatedAt:    time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			LastSignalAt: time.Date(2024, 6, 2, 0, 0, 0, 0, time.UTC),
		}
		b, _ := json.Marshal(rec)
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tx.Put(keys.StoryMeta(id), b)
		}))

		meta, err := tr.Story(id)
		require.NoError(t, err)
		assert.Equal(t, id, meta.ID)
		assert.Equal(t, StoryStateDormant, meta.State)
	})
}

func TestTracker_Ingest(t *testing.T) {
	t.Run("empty_embedding_returns_error", func(t *testing.T) {
		tr := newTestTracker(t)
		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID: uuid.New(), At: time.Now(), Embedding: nil,
		})
		require.Error(t, err)
	})

	t.Run("dimension_mismatch_returns_ErrDimensionMismatch", func(t *testing.T) {
		tr := newTestTracker(t)
		tr.dim.Store(5)

		_, err := tr.Ingest(context.Background(), Signal[string]{
			ID:        uuid.New(),
			At:        time.Now(),
			Embedding: []float32{1, 2, 3}, // dim=3, tracker expects 5
		})
		require.ErrorIs(t, err, ErrDimensionMismatch)
	})

	t.Run("buffers_signal_when_apply_in_progress", func(t *testing.T) {
		tr := newTestTracker(t)
		tr.dim.Store(3)
		tr.applyInProgress.Store(true)

		sig := Signal[string]{ID: uuid.New(), At: time.Now(), Embedding: []float32{1, 2, 3}}
		_, err := tr.Ingest(context.Background(), sig)
		require.NoError(t, err)
		assert.Equal(t, 1, len(tr.ingestBuffer), "signal must be routed to the in-memory buffer")
	})

	t.Run("returns_error_after_close", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{
			Store:         newMemStore(),
			Codec:         JSONCodec[string]{},
			BatchInterval: time.Hour,
		})
		require.NoError(t, err)
		require.NoError(t, tr.Close())

		_, err = tr.Ingest(context.Background(), Signal[string]{
			ID: uuid.New(), At: time.Now(), Embedding: []float32{1},
		})
		require.Error(t, err)
	})
}

// errDecodeFailed is returned by failingDecodeCodec to prove that a codec
// failure reaches the caller unchanged.
var errDecodeFailed = errors.New("codec decode failed")

// failingDecodeCodec encodes normally but always fails to decode.
type failingDecodeCodec[T any] struct{ JSONCodec[T] }

func (failingDecodeCodec[T]) Decode([]byte) (Signal[T], error) {
	return Signal[T]{}, errDecodeFailed
}

func TestTracker_Signal(t *testing.T) {
	t.Run("signal_attached_to_story", func(t *testing.T) {
		tr := newTestTracker(t)
		storyID := uuid.New()
		sigID := uuid.New()
		sig := Signal[string]{
			ID:        sigID,
			At:        time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			Embedding: []float32{1.0, 2.0},
			Data:      "story-data-payload",
		}
		b, err := tr.cfg.Codec.Encode(sig)
		require.NoError(t, err)
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			if err := tx.Put(keys.Signal(storyID, sigID), b); err != nil {
				return err
			}
			return writeSignalLoc(tx, sigID, storyID, false)
		}))

		got, err := tr.Signal(sigID)
		require.NoError(t, err)
		assert.Equal(t, sigID, got.ID)
		assert.Equal(t, "story-data-payload", got.Data)
		assert.Equal(t, sig.Embedding, got.Embedding)
		assert.True(t, sig.At.Equal(got.At))
	})

	t.Run("signal_in_outlier_bucket", func(t *testing.T) {
		tr := newTestTracker(t)
		sigID := uuid.New()
		sig := Signal[string]{
			ID:        sigID,
			At:        time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
			Embedding: []float32{0.5, 0.5},
			Data:      "outlier-data-payload",
		}
		b, err := tr.cfg.Codec.Encode(sig)
		require.NoError(t, err)
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			if err := tx.Put(keys.Outlier(sigID), b); err != nil {
				return err
			}
			return writeSignalLoc(tx, sigID, uuid.Nil, true)
		}))

		got, err := tr.Signal(sigID)
		require.NoError(t, err)
		assert.Equal(t, sigID, got.ID)
		assert.Equal(t, "outlier-data-payload", got.Data)
	})

	t.Run("signal_moved_between_stories_by_batch", func(t *testing.T) {
		tr := newTestTracker(t)
		tr.dim.Store(3)

		// Story A is older, so a batch merge retires B into A and migrates
		// B's signals under A's prefix.
		storyA := uuid.NewSHA1(TrackerNamespace, []byte("signal-merge-A"))
		storyB := uuid.NewSHA1(TrackerNamespace, []byte("signal-merge-B"))
		now := time.Now()
		aEmbs := [][]float32{
			{1.00, 0.01, 0.02},
			{0.99, 0.02, -0.01},
			{1.01, -0.01, 0.01},
			{0.98, 0.03, 0.00},
			{1.00, 0.00, 0.00},
		}
		bEmbs := [][]float32{
			{1.01, 0.00, 0.01},
			{0.99, 0.01, 0.00},
			{1.00, -0.01, 0.02},
		}

		var trackedID uuid.UUID
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			write := func(sid uuid.UUID, created time.Time, embs [][]float32) error {
				for _, emb := range embs {
					sigID := uuid.New()
					sig := Signal[string]{
						ID:        sigID,
						At:        now.Add(-time.Minute),
						Embedding: emb,
						Data:      "moved-signal",
					}
					b, err := tr.cfg.Codec.Encode(sig)
					if err != nil {
						return err
					}
					if err := tx.Put(keys.Signal(sid, sigID), b); err != nil {
						return err
					}
					if err := writeSignalLoc(tx, sigID, sid, false); err != nil {
						return err
					}
					if sid == storyB {
						trackedID = sigID
					}
				}
				return tr.writeStoryMeta(tx, sid, time.Time{}, storyRecord{
					State:        StoryStateActive,
					Centroid:     []float32{1, 0, 0},
					CreatedAt:    created,
					LastSignalAt: now.Add(-time.Minute),
				})
			}
			if err := write(storyA, now.Add(-2*time.Hour), aEmbs); err != nil {
				return err
			}
			return write(storyB, now.Add(-time.Hour), bEmbs)
		}))

		before, err := tr.Signal(trackedID)
		require.NoError(t, err)
		assert.Equal(t, "moved-signal", before.Data)

		tr.runBatch()

		// The signal now lives under story A; Signal must follow the index.
		require.NoError(t, tr.cfg.Store.View(func(tx Tx) error {
			assert.Nil(t, mustGet(t, tx, keys.Signal(storyB, trackedID)), "signal must leave retired story B")
			assert.NotNil(t, mustGet(t, tx, keys.Signal(storyA, trackedID)), "signal must land under survivor story A")
			return nil
		}))

		after, err := tr.Signal(trackedID)
		require.NoError(t, err)
		assert.Equal(t, trackedID, after.ID)
		assert.Equal(t, "moved-signal", after.Data)
	})

	t.Run("unknown_uuid_returns_ErrNotFound", func(t *testing.T) {
		tr := newTestTracker(t)
		_, err := tr.Signal(uuid.New())
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotFound))
	})

	t.Run("dangling_index_returns_ErrNotFound", func(t *testing.T) {
		tr := newTestTracker(t)
		sigID := uuid.New()
		storyID := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return writeSignalLoc(tx, sigID, storyID, false)
		}))

		_, err := tr.Signal(sigID)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotFound))
	})

	t.Run("malformed_index_returns_ErrNotFound", func(t *testing.T) {
		tr := newTestTracker(t)
		sigID := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			return tx.Put(keys.SignalLoc(sigID), []byte("invalid-loc-format"))
		}))

		_, err := tr.Signal(sigID)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrNotFound))
	})

	t.Run("codec_error_propagates_not_ErrNotFound", func(t *testing.T) {
		tr := newTestTracker(t)
		sigID := uuid.New()
		storyID := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			if err := tx.Put(keys.Signal(storyID, sigID), []byte("{invalid json")); err != nil {
				return err
			}
			return writeSignalLoc(tx, sigID, storyID, false)
		}))

		_, err := tr.Signal(sigID)
		require.Error(t, err)
		assert.False(t, errors.Is(err, ErrNotFound))
		assert.Contains(t, err.Error(), "decode signal")
	})

	t.Run("codec_decode_error_propagates_not_ErrNotFound", func(t *testing.T) {
		tr, err := NewTracker[string](Config[string]{
			Store:         newMemStore(),
			Codec:         failingDecodeCodec[string]{},
			BatchInterval: time.Hour,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = tr.Close() })

		sigID := uuid.New()
		storyID := uuid.New()
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			if err := tx.Put(keys.Signal(storyID, sigID), []byte("payload")); err != nil {
				return err
			}
			return writeSignalLoc(tx, sigID, storyID, false)
		}))

		_, err = tr.Signal(sigID)
		require.Error(t, err)
		assert.False(t, errors.Is(err, ErrNotFound))
		assert.True(t, errors.Is(err, errDecodeFailed))
	})

	// Signal must behave like the other read accessors after Close: whatever
	// the store does with reads post-close, Story and Signal agree.
	t.Run("after_close_matches_other_accessors", func(t *testing.T) {
		tr := newTestTracker(t)
		sigID := uuid.New()
		storyID := uuid.New()
		sig := Signal[string]{ID: sigID, Data: "post-close-signal"}
		b, err := tr.cfg.Codec.Encode(sig)
		require.NoError(t, err)
		require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
			if err := tx.Put(keys.Signal(storyID, sigID), b); err != nil {
				return err
			}
			if err := writeSignalLoc(tx, sigID, storyID, false); err != nil {
				return err
			}
			return tr.writeStoryMeta(tx, storyID, time.Time{}, storyRecord{
				State:     StoryStateActive,
				CreatedAt: time.Now(),
			})
		}))

		gotSigBefore, sigErrBefore := tr.Signal(sigID)
		_, storyErrBefore := tr.Story(storyID)

		require.NoError(t, tr.Close())

		gotSigAfter, sigErrAfter := tr.Signal(sigID)
		_, storyErrAfter := tr.Story(storyID)

		assert.Equal(t, sigErrBefore == nil, sigErrAfter == nil, "Signal must not change outcome across Close")
		assert.Equal(t, storyErrBefore == nil, storyErrAfter == nil, "Story must not change outcome across Close")
		assert.Equal(t, sigErrAfter == nil, storyErrAfter == nil, "Signal and Story must agree post-close")
		assert.Equal(t, gotSigBefore, gotSigAfter)

		_, missingSigErr := tr.Signal(uuid.New())
		_, missingStoryErr := tr.Story(uuid.New())
		assert.True(t, errors.Is(missingSigErr, ErrNotFound))
		assert.True(t, errors.Is(missingStoryErr, ErrNotFound))
	})
}
