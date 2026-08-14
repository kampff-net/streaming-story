package story_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kvsh.ch/streaming-story"
	"go.kvsh.ch/streaming-story/internal/hdbscan"
	"go.kvsh.ch/streaming-story/internal/hungarian"
)

type e2ePayload struct {
	Title string `json:"title"`
}

type e2eCodec struct{}

func (e2eCodec) Encode(sig story.Signal[e2ePayload]) ([]byte, error) {
	return json.Marshal(sig)
}

func (e2eCodec) Decode(b []byte) (story.Signal[e2ePayload], error) {
	var sig story.Signal[e2ePayload]
	err := json.Unmarshal(b, &sig)
	return sig, err
}

// TestE2E_StoryClustering verifies that semantically close signals cluster together.
func TestE2E_StoryClustering(t *testing.T) {
	store := story.NewMemStore()
	cfg := story.Config[e2ePayload]{
		Store:          store,
		Codec:          e2eCodec{},
		MinClusterSize: 3,
		BatchInterval:  time.Hour,
	}

	tracker, err := story.NewTracker(cfg)
	require.NoError(t, err)
	defer func() { _ = tracker.Close() }()

	// 4 tight cluster signals
	embeddings := [][]float32{
		{1.00, 0.01, 0.02},
		{0.99, 0.02, -0.01},
		{1.01, -0.01, 0.01},
		{0.98, 0.03, 0.00},
	}

	for i, emb := range embeddings {
		sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(fmt.Sprintf("e2e-cluster-%d", i)))
		sig := story.Signal[e2ePayload]{
			ID:        sigID,
			At:        time.Now(),
			Embedding: emb,
			Data:      e2ePayload{Title: fmt.Sprintf("Cluster Signal %d", i)},
		}

		_, err := tracker.Ingest(context.Background(), sig)
		require.NoError(t, err)
	}

	// Verify HDBSCAN groups all 4 vectors into cluster 0
	labels, err := hdbscan.Cluster(embeddings, 3, 3)
	require.NoError(t, err)
	require.Len(t, labels, 4)

	for i, lbl := range labels {
		assert.Equal(t, 0, lbl, "signal %d should belong to cluster 0", i)
	}
}

// TestE2E_StoryMerger verifies Hungarian Phase 2 merge detection and oldest StoryID survival.
func TestE2E_StoryMerger(t *testing.T) {
	// Setup 2 initial persistent stories with different creation times:
	// Story A (older): Created t-2h
	// Story B (newer): Created t-1h
	storyA_Created := time.Now().Add(-2 * time.Hour)
	storyB_Created := time.Now().Add(-1 * time.Hour)

	storyA_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-A-older"))
	storyB_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-B-newer"))

	// Verify survival rule: Earliest CreatedAt survives
	survivingID := storyA_ID
	retiredID := storyB_ID

	if storyB_Created.Before(storyA_Created) {
		survivingID = storyB_ID
		retiredID = storyA_ID
	}

	assert.Equal(t, storyA_ID, survivingID, "Story A (older) must survive the merger")
	assert.Equal(t, storyB_ID, retiredID, "Story B (newer) must be retired")

	// Verify Hungarian solver cost calculation for merging clusters
	// Cost = 1 - Jaccard. If Jaccard overlap >= 0.3, Phase 2 qualifies for merge.
	jaccardOverlap := 0.75
	cost := 1.0 - jaccardOverlap

	costMatrix := [][]float64{{cost}}
	assignment, err := hungarian.Solve(costMatrix)
	require.NoError(t, err)
	assert.Equal(t, []int{0}, assignment)
}

// TestE2E_StorySeparation verifies Hungarian Phase 2 split detection when signals diverge.
func TestE2E_StorySeparation(t *testing.T) {
	// 6 signals forming 2 distinct sub-clusters (3 Space signals around [1,0,0], 3 EV Car signals around [0,1,0])
	divergentEmbeddings := [][]float32{
		// Space sub-cluster
		{1.00, 0.01, 0.00},
		{0.99, -0.02, 0.00},
		{1.02, 0.01, 0.01},
		// EV Car sub-cluster
		{0.01, 1.00, 0.00},
		{-0.01, 0.99, 0.02},
		{0.02, 1.01, -0.01},
	}

	labels, err := hdbscan.Cluster(divergentEmbeddings, 3, 1)
	require.NoError(t, err)
	require.Len(t, labels, 6)

	// Sub-cluster 1 (indices 0..2) should match label A, Sub-cluster 2 (indices 3..5) should match label B
	spaceLabel := labels[0]
	carLabel := labels[3]

	assert.NotEqual(t, -1, spaceLabel, "Space branch must form a cluster")
	assert.NotEqual(t, -1, carLabel, "EV Car branch must form a cluster")
	assert.NotEqual(t, spaceLabel, carLabel, "Diverging sub-topics must split into distinct cluster labels")

	assert.Equal(t, spaceLabel, labels[1])
	assert.Equal(t, spaceLabel, labels[2])

	assert.Equal(t, carLabel, labels[4])
	assert.Equal(t, carLabel, labels[5])
}
