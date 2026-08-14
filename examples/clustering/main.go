package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story"
	"go.kvsh.ch/streaming-story/internal/hdbscan"
)

type NewsSignal struct {
	Title string `json:"title"`
	Topic string `json:"topic"`
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
	fmt.Println("==========================================================")
	fmt.Println("  Demonstration 1: Multi-Story Real-Time Clustering (4 Stories)")
	fmt.Println("==========================================================")

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
	defer func() { _ = tracker.Close() }()

	// Define 4 distinct semantic topics across 4 orthogonal vector dimensions:
	// Topic 1 (AI Hardware):    [1.0, 0.0, 0.0, 0.0]
	// Topic 2 (Solar Energy):   [0.0, 1.0, 0.0, 0.0]
	// Topic 3 (Mars Rover):     [0.0, 0.0, 1.0, 0.0]
	// Topic 4 (Central Bank):   [0.0, 0.0, 0.0, 1.0]
	sampleSignals := []struct {
		Topic     string
		Title     string
		Embedding []float32
	}{
		// Topic 1: AI Hardware
		{"AI Hardware", "Next-Gen AI Accelerator Unveiled", []float32{1.00, 0.01, 0.02, 0.00}},
		{"AI Hardware", "Chipmaker Reports 3x Performance Gain", []float32{0.99, 0.02, -0.01, 0.00}},
		{"AI Hardware", "Data Centers Adopt New AI Architecture", []float32{1.01, -0.01, 0.01, 0.00}},

		// Topic 2: Solar Energy
		{"Solar Energy", "High-Efficiency Perovskite Solar Cells Breakthrough", []float32{0.01, 1.00, 0.00, 0.01}},
		{"Solar Energy", "Solar Grid Capacity Reaches New Milestone", []float32{-0.01, 0.98, 0.02, 0.00}},
		{"Solar Energy", "Next-Gen Photovoltaics Pass Field Tests", []float32{0.02, 1.02, -0.01, 0.00}},

		// Topic 3: Mars Rover
		{"Topic 3: Mars Rover", "Rover Discovers Ancient Riverbed Formations", []float32{0.00, 0.01, 1.00, 0.02}},
		{"Topic 3: Mars Rover", "Soil Samples Reveal Organic Carbon Traces", []float32{0.01, -0.01, 0.99, -0.01}},
		{"Topic 3: Mars Rover", "Mars Helicopter Completes Record 70th Flight", []float32{-0.02, 0.00, 1.01, 0.01}},

		// Topic 4: Central Bank
		{"Central Bank", "Federal Reserve Signals Rate Cuts Later This Year", []float32{0.00, 0.01, 0.02, 1.00}},
		{"Central Bank", "Inflation Cools Closer to 2% Benchmark Target", []float32{0.01, 0.00, -0.01, 0.98}},
		{"Central Bank", "Bond Yields Drop Following Policy Statement", []float32{-0.01, 0.02, 0.00, 1.02}},
	}

	embeddings := make([][]float32, len(sampleSignals))
	for i, item := range sampleSignals {
		sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(fmt.Sprintf("cluster-demo-sig-%d", i)))
		sig := story.Signal[NewsSignal]{
			ID:        sigID,
			At:        time.Now(),
			Embedding: item.Embedding,
			Data: NewsSignal{
				Title: item.Title,
				Topic: item.Topic,
			},
		}

		embeddings[i] = item.Embedding

		storyID, err := tracker.Ingest(context.Background(), sig)
		if err != nil {
			log.Fatalf("failed to ingest signal %d: %v", i, err)
		}

		fmt.Printf("[%s] Ingested: %-48s | StoryID: %s\n", item.Topic, item.Title, storyID)
	}

	// Execute HDBSCAN clustering over the 4 distinct story clusters
	labels, err := hdbscan.Cluster(embeddings, 3, 1)
	if err != nil {
		log.Fatalf("HDBSCAN clustering error: %v", err)
	}

	clusters := make(map[int][]string)
	for i, lbl := range labels {
		clusters[lbl] = append(clusters[lbl], sampleSignals[i].Title)
	}

	fmt.Println("\n----------------------------------------------------------")
	fmt.Printf("HDBSCAN Discovered %d Disjoint Story Clusters:\n", len(clusters))
	fmt.Println("----------------------------------------------------------")

	for label, titles := range clusters {
		fmt.Printf("\nStory Cluster #%d (%d signals):\n", label, len(titles))
		for _, title := range titles {
			fmt.Printf("  • %s\n", title)
		}
	}
}
