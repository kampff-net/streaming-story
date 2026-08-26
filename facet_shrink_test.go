package story

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Spec 007 fixes a signal's facet set at first ingest, but a producer may still
// re-deliver one with fewer facets. That delivery genuinely removes facets, and
// these tests pin the removal down to the last key: the failure it guards
// against is silent, permanent, and invisible to every read path — markers left
// under a signal whose record has been collected, which no scan can resolve and
// no eviction can reach.

// storeKeys returns every key in the store, for tests that assert on what the
// store holds rather than on what a read path reports.
func storeKeys(t *testing.T, tr *Tracker[string]) []string {
	t.Helper()
	var out []string
	require.NoError(t, tr.cfg.Store.ScanPrefix([]byte(""), func(key, _ []byte) error {
		out = append(out, string(key))
		return nil
	}))
	return out
}

// keysWithPrefix filters storeKeys down to one key space.
func keysWithPrefix(keys []string, prefix string) []string {
	var out []string
	for _, k := range keys {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	return out
}

func shrinkTracker(t *testing.T) *Tracker[string] {
	t.Helper()
	tr, err := NewTracker[string](Config[string]{
		Store:         newMemStore(),
		BatchInterval: time.Hour, // the ticker must not fire mid-test
		MinStorySize:  2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = tr.Close() })
	return tr
}

func TestIngest_ShrinkDropsUnplacedFacets(t *testing.T) {
	tr := shrinkTracker(t)
	id := uuid.New()
	now := time.Now()
	three := []Embedding{{1, 0}, {0, 1}, {1, 1}}

	_, err := tr.Ingest(context.Background(), Signal[string]{ID: id, At: now, Embeddings: three, Data: "v1"})
	require.NoError(t, err)
	require.Len(t, keysWithPrefix(storeKeys(t, tr), "o:"), 3, "all three facets start unplaced")

	_, err = tr.Ingest(context.Background(), Signal[string]{ID: id, At: now, Embeddings: three[:1], Data: "v2"})
	require.NoError(t, err)

	require.Len(t, keysWithPrefix(storeKeys(t, tr), "o:"), 1, "facets 1 and 2 are gone from the outlier bucket")

	sig, err := tr.Signal(id)
	require.NoError(t, err)
	require.Len(t, sig.Embeddings, 1, "the record no longer carries the dropped vectors")
	require.Equal(t, "v1", sig.Data, "the payload stays write-once")
	storeInvariants(t, tr)
}

func TestIngest_ShrinkDropsPlacedFacets(t *testing.T) {
	tr := shrinkTracker(t)
	now := time.Now()
	three := []Embedding{{1, 0}, {0, 1}, {1, 1}}

	// Two signals sharing all three facet directions, so a batch run forms a
	// story around each direction and every facet is placed.
	id := uuid.New()
	for _, sid := range []uuid.UUID{id, uuid.New()} {
		_, err := tr.Ingest(context.Background(), Signal[string]{ID: sid, At: now, Embeddings: three})
		require.NoError(t, err)
	}
	tr.runBatch()

	placed, err := tr.FacetsOfSignal(id)
	require.NoError(t, err)
	require.Len(t, placed, 3, "precondition: all three facets are placed in stories")

	_, err = tr.Ingest(context.Background(), Signal[string]{ID: id, At: now, Embeddings: three[:1]})
	require.NoError(t, err)

	placed, err = tr.FacetsOfSignal(id)
	require.NoError(t, err)
	require.Len(t, placed, 1, "the dropped facets left their stories")
	require.Equal(t, 0, placed[0].Facet)

	// The membership markers themselves are gone, not merely unreported.
	for _, k := range storeKeys(t, tr) {
		if len(k) > 2 && k[0] == 's' {
			require.NotContains(t, k, id.String()+":0001")
			require.NotContains(t, k, id.String()+":0002")
		}
	}
	storeInvariants(t, tr)
}

// TestIngest_ShrinkThenEvictLeavesNothing is the regression proper. A shrunk
// signal that later ages out must clear the store as completely as one that was
// never shrunk: before the fix its dropped facets survived eviction as markers
// whose canonical record had already been collected.
func TestIngest_ShrinkThenEvictLeavesNothing(t *testing.T) {
	three := []Embedding{{1, 0}, {0, 1}, {1, 1}}
	aged := time.Now().Add(-100 * 24 * time.Hour) // well past the default OutlierTTL

	run := func(shrink bool) []string {
		tr := shrinkTracker(t)
		id := uuid.New()
		_, err := tr.Ingest(context.Background(), Signal[string]{ID: id, At: aged, Embeddings: three})
		require.NoError(t, err)
		if shrink {
			_, err = tr.Ingest(context.Background(), Signal[string]{ID: id, At: aged, Embeddings: three[:1]})
			require.NoError(t, err)
		}
		// The first run establishes lastBatch; the TTL is measured against it,
		// so eviction happens on the second.
		tr.runBatch()
		tr.runBatch()
		storeInvariants(t, tr)
		return storeKeys(t, tr)
	}

	require.Equal(t, run(false), run(true),
		"a shrunk signal must age out as cleanly as one that was never shrunk")
}

// TestEvictOutlierFacets_MarkersAreAuthoritative covers the defence behind the
// fix: eviction reads the markers, not the location index. The index is derived
// state, so a stale or truncated one must not cost the store a marker.
func TestEvictOutlierFacets_MarkersAreAuthoritative(t *testing.T) {
	tr := shrinkTracker(t)
	id := uuid.New()
	_, err := tr.Ingest(context.Background(), Signal[string]{
		ID: id, At: time.Now(), Embeddings: []Embedding{{1, 0}, {0, 1}, {1, 1}},
	})
	require.NoError(t, err)

	// Truncate the location index behind the tracker's back.
	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return writeSignalLocSet(tx, id, nil)
	}))

	require.NoError(t, tr.cfg.Store.Update(func(tx Tx) error {
		return evictOutlierFacets(tx, id)
	}))

	require.Empty(t, keysWithPrefix(storeKeys(t, tr), "o:"), "every marker was evicted")
	require.Empty(t, keysWithPrefix(storeKeys(t, tr), "g:"), "the record went with the last facet")
}
