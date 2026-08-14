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

// ArticlePayload represents a generic caller data structure attached to a signal.
type ArticlePayload struct {
	Title   string `json:"title"`
	Source  string `json:"source"`
	Content string `json:"content"`
}

// JSONCodec implements story.Codec[ArticlePayload] for persistence.
type JSONCodec struct{}

func (c JSONCodec) Encode(sig story.Signal[ArticlePayload]) ([]byte, error) {
	return json.Marshal(sig)
}

func (c JSONCodec) Decode(b []byte) (story.Signal[ArticlePayload], error) {
	var sig story.Signal[ArticlePayload]
	err := json.Unmarshal(b, &sig)
	return sig, err
}

func main() {
	// Create mock in-memory store for demonstration
	store := story.NewMemStore()

	// Configure the Tracker
	cfg := story.Config[ArticlePayload]{
		Store:         store,
		Codec:         JSONCodec{},
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
		ID:        sigID,
		At:        time.Now(),
		Embedding: []float32{0.15, -0.32, 0.77, 0.05, 0.41},
		Data: ArticlePayload{
			Title:   "New AI Chip Breakthrough Announced",
			Source:  "Tech Daily",
			Content: "Researchers unveil new ultra-efficient hardware architecture.",
		},
	}

	// Ingest signal real-time (Draft Phase)
	storyID, err := tracker.Ingest(context.Background(), signal)
	if err != nil {
		log.Fatalf("failed to ingest signal: %v", err)
	}

	fmt.Printf("Signal %s ingested. Assigned provisional Story ID: %s\n", signal.ID, storyID)
}
