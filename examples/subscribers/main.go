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

type NewsPayload struct {
	Headline string `json:"headline"`
	Topic    string `json:"topic"`
}

type NewsCodec struct{}

func (c NewsCodec) Encode(sig story.Signal[NewsPayload]) ([]byte, error) {
	return json.Marshal(sig)
}

func (c NewsCodec) Decode(b []byte) (story.Signal[NewsPayload], error) {
	var sig story.Signal[NewsPayload]
	err := json.Unmarshal(b, &sig)
	return sig, err
}

func main() {
	store := story.NewMemStore()
	cfg := story.Config[NewsPayload]{
		Store:         store,
		Codec:         NewsCodec{},
		BatchInterval: 500 * time.Millisecond,
		EventBufferSize: 128,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()

	// Subscribe to event stream
	eventCh := tracker.Subscribe()

	// Start subscriber listener goroutine
	go func() {
		for ev := range eventCh {
			switch ev.Kind {
			case story.EventDraftAssigned:
				fmt.Printf("[EVENT] Real-time draft assignment: Signal %s -> Story %s\n", ev.SignalID, ev.StoryID)
			case story.EventStoryCreated:
				fmt.Printf("[EVENT] New story created: Story %s\n", ev.StoryID)
			case story.EventStoryMerged:
				fmt.Printf("[EVENT] Story merge: %s merged into %s\n", ev.StoryID2, ev.StoryID)
			case story.EventStorySplit:
				fmt.Printf("[EVENT] Story split: %s split to %s\n", ev.StoryID, ev.StoryID2)
			case story.EventBatchComplete:
				if ev.BatchSummary != nil {
					fmt.Printf("[EVENT] Batch complete: %d created, %d merged, %d split, %d reassigned\n",
						ev.BatchSummary.StoriesCreated,
						ev.BatchSummary.StoriesMerged,
						ev.BatchSummary.StoriesSplit,
						ev.BatchSummary.SignalsReassigned,
					)
				}
			case story.EventBufferOverflow:
				fmt.Println("[EVENT] WARNING: Subscriber buffer overflowed, events dropped!")
			}
		}
	}()

	// Ingest sample signal
	sigID := uuid.NewSHA1(story.TrackerNamespace, []byte("news-signal-001"))
	sig := story.Signal[NewsPayload]{
		ID:        sigID,
		At:        time.Now(),
		Embedding: []float32{0.88, 0.12, 0.44},
		Data: NewsPayload{
			Headline: "Central Bank Adjusts Interest Rates",
			Topic:    "Finance",
		},
	}

	_, _ = tracker.Ingest(context.Background(), sig)
	time.Sleep(100 * time.Millisecond)
}
