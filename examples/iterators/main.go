package main

import (
	"fmt"
	"log"

	"go.kvsh.ch/streaming-story"
)

type SimplePayload struct {
	Text string `cbor:"0,keyasint"`
}

func main() {
	store := story.NewMemStore()
	cfg := story.Config[SimplePayload]{
		Store: store,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to create tracker: %v", err)
	}
	defer func() { _ = tracker.Close() }()

	fmt.Println("Iterating over all active stories (Go 1.22 range-over-func)...")

	// Demonstrate Go 1.22 iterator usage over stories
	for meta := range tracker.Stories(story.StoryStateActive) {
		fmt.Printf("Story ID: %s | State: %d | Radius: %.4f\n", meta.ID, meta.State, meta.Radius)

		// Iterate over all signals belonging to story
		for sig, err := range tracker.SignalsOf(meta.ID) {
			if err != nil {
				log.Printf("error reading signal: %v\n", err)
				continue
			}
			fmt.Printf("  └─ Signal ID: %s | Payload: %s\n", sig.ID, sig.Data.Text)
		}
	}
}
