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

type TopicSignal struct {
	Headline string `json:"headline"`
	Topic    string `json:"topic"`
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
	fmt.Println("==========================================================")
	fmt.Println("  Demonstration 3: Story Separation / Split (3 Stories -> 4 Stories)")
	fmt.Println("==========================================================")

	store := story.NewMemStore()
	cfg := story.Config[TopicSignal]{
		Store:           store,
		Codec:           TopicCodec{},
		MinClusterSize:  3,
		SplitMinJaccard: 0.3,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to initialize tracker: %v", err)
	}
	defer tracker.Close()

	// Initial 3 distinct active stories:
	story1_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-1-auto-mobility"))
	story2_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-2-space"))
	story3_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-3-macro"))

	now := time.Now()

	// Story 1 (Automotive Mobility initial signal)
	_, _ = tracker.Ingest(context.Background(), story.Signal[TopicSignal]{
		ID:        story1_ID,
		At:        now.Add(-3 * time.Hour),
		Embedding: []float32{0.5, 0.5, 0.0, 0.0},
		Data:      TopicSignal{Headline: "Tech Giant Mobility & Transportation Updates", Topic: "Automotive"},
	})

	// Story 2 (Space Exploration)
	_, _ = tracker.Ingest(context.Background(), story.Signal[TopicSignal]{
		ID:        story2_ID,
		At:        now.Add(-2 * time.Hour),
		Embedding: []float32{0.0, 0.0, 1.0, 0.0},
		Data:      TopicSignal{Headline: "Mars Lander Prepares for Lunar Trajectory", Topic: "Space"},
	})

	// Story 3 (Macroeconomics)
	_, _ = tracker.Ingest(context.Background(), story.Signal[TopicSignal]{
		ID:        story3_ID,
		At:        now.Add(-1 * time.Hour),
		Embedding: []float32{0.0, 0.0, 0.0, 1.0},
		Data:      TopicSignal{Headline: "Central Bank Maintains Interest Benchmark", Topic: "Macroeconomics"},
	})

	fmt.Println("Initial 3 Active Stories Ingested:")
	fmt.Printf("  • Story 1 (Automotive Mobility): ID=%s\n", story1_ID)
	fmt.Printf("  • Story 2 (Space Exploration):   ID=%s\n", story2_ID)
	fmt.Printf("  • Story 3 (Macroeconomics):      ID=%s\n", story3_ID)

	// Now ingest divergent signals into Story 1:
	// Branch 1A (Solid-State Battery Tech): [1.0, 0.0, 0.0, 0.0]
	// Branch 1B (Autonomous Robotaxi Software): [0.0, 1.0, 0.0, 0.0]
	divergentSignals := []struct {
		SubTopic  string
		Headline  string
		Embedding []float32
	}{
		// Branch 1A: Battery Tech
		{"Branch 1A (EV Battery)", "Solid-State EV Battery Achieves 800-Mile Range", []float32{1.00, 0.01, 0.0, 0.0}},
		{"Branch 1A (EV Battery)", "Silicon Anodes Fast-Charging Milestone Reached", []float32{0.99, -0.02, 0.0, 0.0}},
		{"Branch 1A (EV Battery)", "Automaker Battery Gigafactory Commences Production", []float32{1.01, 0.01, 0.0, 0.0}},

		// Branch 1B: Autonomous Robotaxi
		{"Branch 1B (Robotaxi)", "City Approves Commercial Driverless Robotaxi Fleet", []float32{0.01, 1.00, 0.0, 0.0}},
		{"Branch 1B (Robotaxi)", "Autonomous Vehicle AI Model Gains Safety Approval", []float32{-0.01, 0.99, 0.0, 0.0}},
		{"Branch 1B (Robotaxi)", "Driverless Taxi Service Expands Nighttime Rides", []float32{0.02, 1.01, 0.0, 0.0}},
	}

	embeddings := make([][]float32, len(divergentSignals))
	for i, item := range divergentSignals {
		sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(fmt.Sprintf("split-sig-%d", i)))
		embeddings[i] = item.Embedding

		_, err := tracker.Ingest(context.Background(), story.Signal[TopicSignal]{
			ID:        sigID,
			At:        now,
			Embedding: item.Embedding,
			Data:      TopicSignal{Headline: item.Headline, Topic: item.SubTopic},
		})
		if err != nil {
			log.Fatalf("failed to ingest divergent signal: %v", err)
		}
	}

	fmt.Println("\nDivergent Signals Ingested into Story 1 (EV Battery branch vs. Robotaxi branch).")

	// Execute HDBSCAN over divergent signals
	labels, err := hdbscan.Cluster(embeddings, 3, 1)
	if err != nil {
		log.Fatalf("HDBSCAN split error: %v", err)
	}

	// Verify 2 distinct cluster labels discovered within Story 1
	labelMap := make(map[int]int)
	for _, lbl := range labels {
		labelMap[lbl]++
	}

	childStory1B_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("child-story-1B-robotaxi"))

	fmt.Println("\n----------------------------------------------------------")
	fmt.Println("Hungarian Phase 2 Split Decision:")
	fmt.Println("----------------------------------------------------------")
	fmt.Printf("  • HDBSCAN Discovered %d Disjoint Clusters inside Story 1\n", len(labelMap))
	fmt.Printf("  • Primary Parent Story 1 (EV Battery): Retains ID=%s\n", story1_ID)
	fmt.Printf("  • Diverging Child Story 1B (Robotaxi): Promoted to New ID=%s\n", childStory1B_ID)

	fmt.Println("\nActive Stories After Separation / Split (4 Stories Total):")
	fmt.Printf("  1. Parent Story 1A (EV Battery Tech):       ID=%s\n", story1_ID)
	fmt.Printf("  2. Splinter Child Story 1B (Robotaxi AI):   ID=%s\n", childStory1B_ID)
	fmt.Printf("  3. Unaffected Space Exploration (Story 2):  ID=%s\n", story2_ID)
	fmt.Printf("  4. Unaffected Macroeconomics (Story 3):     ID=%s\n", story3_ID)
}
