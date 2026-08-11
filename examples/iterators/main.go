package main

import (
	"encoding/json"
	"fmt"
	"log"

	"go.kvsh.ch/streaming-story"
)

type SimplePayload struct {
	Text string `json:"text"`
}

type SimpleCodec struct{}

func (c SimpleCodec) Encode(sig story.Signal[SimplePayload]) ([]byte, error) {
	return json.Marshal(sig)
}

func (c SimpleCodec) Decode(b []byte) (story.Signal[SimplePayload], error) {
	var sig story.Signal[SimplePayload]
	err := json.Unmarshal(b, &sig)
	return sig, err
}

func main() {
	store := story.NewMemStore()
	cfg := story.Config[SimplePayload]{
		Store: store,
		Codec: SimpleCodec{},
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to create tracker: %v", err)
	}
	defer tracker.Close()

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
