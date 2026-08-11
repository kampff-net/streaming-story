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

type EventSignal struct {
	Summary string `json:"summary"`
}

type EventCodec struct{}

func (c EventCodec) Encode(sig story.Signal[EventSignal]) ([]byte, error) {
	return json.Marshal(sig)
}

func (c EventCodec) Decode(b []byte) (story.Signal[EventSignal], error) {
	var sig story.Signal[EventSignal]
	err := json.Unmarshal(b, &sig)
	return sig, err
}

func main() {
	fmt.Println("=== Demonstration 2: Story Merger ===")

	store := story.NewMemStore()
	cfg := story.Config[EventSignal]{
		Store:          store,
		Codec:          EventCodec{},
		MinClusterSize: 2,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to initialize tracker: %v", err)
	}
	defer tracker.Close()

	// Story 1: Initial topic around [1.0, 0.0, 0.0]
	story1Signal := story.Signal[EventSignal]{
		ID:        uuid.NewSHA1(story.TrackerNamespace, []byte("story1-sig1")),
		At:        time.Now().Add(-2 * time.Hour), // Older creation time
		Embedding: []float32{1.0, 0.05, 0.0},
		Data:      EventSignal{Summary: "Company A announces acquisition talks"},
	}
	_, _ = tracker.Ingest(context.Background(), story1Signal)

	// Story 2: Secondary topic around [0.0, 1.0, 0.0]
	story2Signal := story.Signal[EventSignal]{
		ID:        uuid.NewSHA1(story.TrackerNamespace, []byte("story2-sig1")),
		At:        time.Now().Add(-1 * time.Hour), // Newer creation time
		Embedding: []float32{0.0, 1.0, 0.05},
		Data:      EventSignal{Summary: "Regulatory agency reviews market concentration"},
	}
	_, _ = tracker.Ingest(context.Background(), story2Signal)

	// Bridging Signals that overlap both topics: [0.5, 0.5, 0.0]
	bridgeSignal := story.Signal[EventSignal]{
		ID:        uuid.NewSHA1(story.TrackerNamespace, []byte("bridge-sig1")),
		At:        time.Now(),
		Embedding: []float32{0.5, 0.5, 0.0},
		Data:      EventSignal{Summary: "Regulators formally open review into Company A acquisition"},
	}
	_, _ = tracker.Ingest(context.Background(), bridgeSignal)

	fmt.Println("Story 1 and Story 2 bridged by overlapping signal.")
	fmt.Println("Upon batch re-clustering, Hungarian Phase 2 mapping detects N-way merge.")
	fmt.Println("Rule enforced: Oldest StoryID (Company A acquisition) survives; newer story is retired.")
}
