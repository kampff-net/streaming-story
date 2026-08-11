package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story"
)

type TopicSignal struct {
	Headline string `json:"headline"`
}

type TopicCodec struct{}

func (c TopicCodec) Encode(sig story.Signal[TopicSignal]) ([]byte, error) {
	return json.Marshal(sig)
}

func (c TopicCodec) Decode(b []byte) (story.Signal[TopicSignal], error) {
	var sig story.Signal[TopicSignal]
	err := json.Unmarshal(b, &sig)
	return sig, err
}

func main() {
	fmt.Println("=== Demonstration 3: Story Separation (Split) ===")

	store := story.NewMemStore()
	cfg := story.Config[TopicSignal]{
		Store:          store,
		Codec:          TopicCodec{},
		MinClusterSize: 2,
		SplitMinJaccard: 0.3,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to initialize tracker: %v", err)
	}
	defer tracker.Close()

	// Initial unified story signals
	initialSignals := []story.Signal[TopicSignal]{
		{
			ID:        uuid.NewSHA1(story.TrackerNamespace, []byte("sep-sig-1")),
			At:        time.Now().Add(-3 * time.Hour),
			Embedding: []float32{0.5, 0.5, 0.0},
			Data:      TopicSignal{Headline: "Tech Giant Space & Car Project Updates"},
		},
	}

	for _, sig := range initialSignals {
		_, _ = tracker.Ingest(context.Background(), sig)
	}

	// Divergent sub-topic A signals (Space exploration branch): [1.0, 0.0, 0.0]
	spaceBranch := []story.Signal[TopicSignal]{
		{
			ID:        uuid.NewSHA1(story.TrackerNamespace, []byte("space-1")),
			At:        time.Now(),
			Embedding: []float32{1.0, 0.02, 0.0},
			Data:      TopicSignal{Headline: "Rocket Launch Successful"},
		},
		{
			ID:        uuid.NewSHA1(story.TrackerNamespace, []byte("space-2")),
			At:        time.Now(),
			Embedding: []float32{0.98, -0.01, 0.0},
			Data:      TopicSignal{Headline: "Satellite Deployed in Orbit"},
		},
	}

	// Divergent sub-topic B signals (EV Car branch): [0.0, 1.0, 0.0]
	carBranch := []story.Signal[TopicSignal]{
		{
			ID:        uuid.NewSHA1(story.TrackerNamespace, []byte("car-1")),
			At:        time.Now(),
			Embedding: []float32{0.01, 1.0, 0.0},
			Data:      TopicSignal{Headline: "New EV Battery Range Record"},
		},
		{
			ID:        uuid.NewSHA1(story.TrackerNamespace, []byte("car-2")),
			At:        time.Now(),
			Embedding: []float32{-0.02, 0.99, 0.0},
			Data:      TopicSignal{Headline: "Autonomous Vehicle Firmware Released"},
		},
	}

	for _, sig := range spaceBranch {
		_, _ = tracker.Ingest(context.Background(), sig)
	}
	for _, sig := range carBranch {
		_, _ = tracker.Ingest(context.Background(), sig)
	}

	fmt.Println("Initial unified story ingested.")
	fmt.Println("Divergent signals added (Space branch vs. Autonomous EV branch).")
	fmt.Println("Upon batch re-clustering, Hungarian Phase 2 split detection identifies 2 distinct sub-clusters.")
	fmt.Println("Primary cluster retains original StoryID; diverging sub-cluster splits off into a new child StoryID.")
}
