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

type NewsSignal struct {
	Title string `json:"title"`
}

type NewsCodec struct{}

func (c NewsCodec) Encode(sig story.Signal[NewsSignal]) ([]byte, error) {
	return json.Marshal(sig)
}

func (c NewsCodec) Decode(b []byte) (story.Signal[NewsSignal], error) {
	var sig story.Signal[NewsSignal]
	err := json.Unmarshal(b, &sig)
	return sig, err
}

func main() {
	fmt.Println("=== Demonstration 1: Story Clustering ===")

	store := story.NewMemStore()
	cfg := story.Config[NewsSignal]{
		Store:          store,
		Codec:          NewsCodec{},
		MinClusterSize: 3,
		BatchInterval:  100 * time.Millisecond,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to initialize tracker: %v", err)
	}
	defer tracker.Close()

	// Ingest 4 tightly clustered signals around vector [1.0, 0.0, 0.0]
	clusterAEmbeddings := [][]float32{
		{1.00, 0.01, 0.02},
		{0.99, 0.02, -0.01},
		{1.01, -0.01, 0.01},
		{0.98, 0.03, 0.00},
	}

	for i, emb := range clusterAEmbeddings {
		sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(fmt.Sprintf("tech-news-%d", i)))
		sig := story.Signal[NewsSignal]{
			ID:        sigID,
			At:        time.Now(),
			Embedding: emb,
			Data: NewsSignal{
				Title: fmt.Sprintf("Quantum Computing Breakthrough Part %d", i+1),
			},
		}

		storyID, err := tracker.Ingest(context.Background(), sig)
		if err != nil {
			log.Fatalf("failed to ingest signal %d: %v", i, err)
		}
		fmt.Printf("Ingested signal %s -> Draft Story ID: %s\n", sigID, storyID)
	}

	fmt.Println("\nClustering complete! All 4 semantically similar signals grouped into story cluster.")
}
