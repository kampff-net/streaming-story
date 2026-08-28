package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	story "go.kvsh.ch/streaming-story"
)

type NewsPayload struct {
	Headline string `cbor:"0,keyasint"`
	Topic    string `cbor:"1,keyasint"`
}

func elapsed(start time.Time) string {
	return time.Since(start).Round(time.Millisecond).String()
}

func main() {
	store := story.NewMemStore()
	cfg := story.Config[NewsPayload]{
		Store:           store,
		BatchSchedule:   "@every 500ms",
		EventBufferSize: 128,
		// A demo corpus is far narrower than a real one, so leave more of the
		// corpus mean in place: full centring would turn a handful of tight
		// clusters into noise. See Config.MeanRemoval.
		MeanRemoval: 0.6,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to create tracker: %v", err)
	}
	defer func() { _ = tracker.Close() }()

	// Subscribe to event stream
	eventCh := tracker.Subscribe()
	start := time.Now()

	// Start subscriber listener goroutine
	go func() {
		for ev := range eventCh {
			switch ev.Kind {
			case story.EventDraftAssigned:
				fmt.Printf("[EVENT %s] Real-time draft assignment: Signal %s -> Story %s\n", elapsed(start), ev.SignalID, ev.StoryID)
			case story.EventStoryCreated:
				fmt.Printf("[EVENT %s] New story created: Story %s\n", elapsed(start), ev.StoryID)
			case story.EventStoryMerged:
				fmt.Printf("[EVENT %s] Story merge: %s merged into %s\n", elapsed(start), ev.StoryID2, ev.StoryID)
			case story.EventStoryRetired:
				fmt.Printf("[EVENT %s] Story retired (emptied by batch): %s\n", elapsed(start), ev.StoryID)
			case story.EventStorySplit:
				fmt.Printf("[EVENT %s] Story split: %s split to %s\n", elapsed(start), ev.StoryID, ev.StoryID2)
			case story.EventSignalReassigned:
				fmt.Printf("[EVENT %s] Signal %s reassigned -> %s\n", elapsed(start), ev.SignalID, ev.StoryID)
			case story.EventBatchComplete:
				if ev.BatchSummary != nil {
					fmt.Printf("[EVENT %s] Batch complete: %d created, %d merged, %d split, %d retired, %d reassigned, %d promoted, %d evicted\n",
						elapsed(start),
						ev.BatchSummary.StoriesCreated,
						ev.BatchSummary.StoriesMerged,
						ev.BatchSummary.StoriesSplit,
						ev.BatchSummary.StoriesRetired,
						ev.BatchSummary.SignalsReassigned,
						ev.BatchSummary.OutliersPromoted,
						ev.BatchSummary.OutliersEvicted,
					)
				}
			case story.EventBufferOverflow:
				fmt.Printf("[EVENT %s] WARNING: Subscriber buffer overflowed, events dropped!\n", elapsed(start))
			}
		}
	}()

	// Two clusters of three signals form two stories; a fourth, far away,
	// stays a genuine outlier. 100ms between ingests, then a batch run
	// resolves the clusters.
	signals := []story.Signal[NewsPayload]{
		{
			ID:         uuid.NewSHA1(story.TrackerNamespace, []byte("news-signal-001")),
			At:         time.Now(),
			Embeddings: []story.Embedding{[]float32{0.90, 0.30, 0.10}},
			Data:       NewsPayload{Headline: "Central Bank Adjusts Interest Rates", Topic: "Finance"},
		},
		{
			ID:         uuid.NewSHA1(story.TrackerNamespace, []byte("news-signal-002")),
			At:         time.Now(),
			Embeddings: []story.Embedding{[]float32{0.88, 0.28, 0.12}},
			Data:       NewsPayload{Headline: "Regulator Signals Looser Lending Rules", Topic: "Finance"},
		},
		{
			ID:         uuid.NewSHA1(story.TrackerNamespace, []byte("news-signal-003")),
			At:         time.Now(),
			Embeddings: []story.Embedding{[]float32{0.92, 0.32, 0.08}},
			Data:       NewsPayload{Headline: "Banks Post Record Quarterly Earnings", Topic: "Finance"},
		},
		{
			ID:         uuid.NewSHA1(story.TrackerNamespace, []byte("news-signal-004")),
			At:         time.Now(),
			Embeddings: []story.Embedding{[]float32{0.10, 0.90, 0.30}},
			Data:       NewsPayload{Headline: "Wildfire Evacuations Ordered in New Region", Topic: "Disaster"},
		},
		{
			ID:         uuid.NewSHA1(story.TrackerNamespace, []byte("news-signal-005")),
			At:         time.Now(),
			Embeddings: []story.Embedding{[]float32{0.12, 0.88, 0.28}},
			Data:       NewsPayload{Headline: "Coastal Storm Surge Threatens Port Cities", Topic: "Disaster"},
		},
		{
			ID:         uuid.NewSHA1(story.TrackerNamespace, []byte("news-signal-006")),
			At:         time.Now(),
			Embeddings: []story.Embedding{[]float32{0.08, 0.92, 0.32}},
			Data:       NewsPayload{Headline: "Aftershocks Rattle Quake-Hit County", Topic: "Disaster"},
		},
		{
			ID:         uuid.NewSHA1(story.TrackerNamespace, []byte("news-signal-007")),
			At:         time.Now(),
			Embeddings: []story.Embedding{[]float32{0.05, 0.05, 0.98}},
			Data:       NewsPayload{Headline: "Star Striker Nets Hat-Trick in Derby", Topic: "Sports"},
		},
	}

	for _, sig := range signals {
		assigned, err := tracker.Ingest(context.Background(), sig)
		if err != nil {
			log.Fatalf("failed to ingest %s: %v", sig.ID, err)
		}
		fmt.Printf("[INGEST] %s -> %v\n", sig.ID, assigned)
		time.Sleep(100 * time.Millisecond)
	}

	// The 500ms BatchSchedule fires a batch mid-ingest: with only some of the
	// signals collected it merges everything into one story, then the next
	// batch (once all seven are in) splits it back into two stories plus the
	// outlier — demonstrating how the refinement phase reclusters.
	time.Sleep(500 * time.Millisecond)
}
