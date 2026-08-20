package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story"
)

// ArticlePayload represents a generic caller data structure attached to a signal.
type ArticlePayload struct {
	Title   string `cbor:"0,keyasint"`
	Source  string `cbor:"1,keyasint"`
	Content string `cbor:"2,keyasint"`
}

func main() {
	// Create mock in-memory store for demonstration
	store := story.NewMemStore()

	// Configure the Tracker
	cfg := story.Config[ArticlePayload]{
		Store:         store,
		BatchWindow:   24 * time.Hour,
		BatchInterval: 30 * time.Minute,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to create tracker: %v", err)
	}
	defer func() { _ = tracker.Close() }()

	// Derive a stable UUID v5 signal ID using TrackerNamespace
	domainKey := "article-tech-2026-001"
	sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(domainKey))

	signal := story.Signal[ArticlePayload]{
		ID:         sigID,
		At:         time.Now(),
		Embeddings: []story.Embedding{[]float32{0.15, -0.32, 0.77, 0.05, 0.41}},
		Data: ArticlePayload{
			Title:   "New AI Chip Breakthrough Announced",
			Source:  "Tech Daily",
			Content: "Researchers unveil new ultra-efficient hardware architecture.",
		},
	}

	// Ingest signal real-time (Draft Phase). A signal joins one story per facet
	// it places, so the result is a set: empty when no story claimed any facet.
	storyIDs, err := tracker.Ingest(context.Background(), signal)
	if err != nil {
		log.Fatalf("failed to ingest signal: %v", err)
	}

	if len(storyIDs) == 0 {
		fmt.Printf("Signal %s ingested. No story matched; held as an outlier.\n", signal.ID)
	} else {
		fmt.Printf("Signal %s ingested. Assigned provisional stories: %v\n", signal.ID, storyIDs)
	}
}
