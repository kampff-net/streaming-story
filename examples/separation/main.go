package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story"
	"go.kvsh.ch/streaming-story/internal/dist"
	"go.kvsh.ch/streaming-story/internal/hdbscan"
)

type TopicSignal struct {
	Title string `json:"title"`
	Topic string `json:"topic"`
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
	fmt.Println("=========================================================================")
	fmt.Println("  Story Separation / Split Demonstration — 4-Stage Lifecycle")
	fmt.Println("=========================================================================")

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
	defer func() { _ = tracker.Close() }()

	now := time.Now()

	// -------------------------------------------------------------------------
	// STAGE 1: Ingest Initial Signals into Blank Store (Before Clustering)
	// -------------------------------------------------------------------------
	wave1Signals := []struct {
		ID        string
		Topic     string
		Title     string
		Embedding []float32
	}{
		// Topic 1: Automotive Mobility (Unified initial cluster)
		{"auto-1", "Automotive Mobility", "Tech Giant Mobility & Transportation Updates", []float32{0.50, 0.50, 0.00, 0.00}},
		{"auto-2", "Automotive Mobility", "Automotive Sector Quarterly Operations Report", []float32{0.49, 0.51, 0.00, 0.00}},
		{"auto-3", "Automotive Mobility", "Vehicle Manufacturing Line Automation Milestone", []float32{0.51, 0.49, 0.00, 0.00}},

		// Topic 2: Space Exploration
		{"space-1", "Space Exploration", "Mars Lander Prepares for Lunar Trajectory", []float32{0.00, 0.00, 1.00, 0.01}},
		{"space-2", "Space Exploration", "Deep Space Telescope Detects Exoplanet Atmosphere", []float32{0.00, 0.00, 0.99, -0.01}},
		{"space-3", "Space Exploration", "Rocket Propulsion Test Complete at Spaceport", []float32{0.00, 0.00, 1.01, 0.02}},

		// Topic 3: Macroeconomics
		{"macro-1", "Macroeconomics", "Central Bank Maintains Interest Benchmark Rate", []float32{0.00, 0.00, 0.00, 1.00}},
		{"macro-2", "Macroeconomics", "Consumer Price Index Inflation Numbers Released", []float32{0.00, 0.00, 0.01, 0.98}},
		{"macro-3", "Macroeconomics", "Treasury Department Issues Yield Curve Forecast", []float32{0.00, 0.00, -0.01, 1.02}},
	}

	wave1SigMap := make(map[uuid.UUID]story.Signal[TopicSignal])
	wave1Embeddings := make([][]float32, len(wave1Signals))

	for i, s := range wave1Signals {
		sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(s.ID))
		sig := story.Signal[TopicSignal]{
			ID:        sigID,
			At:        now.Add(-3 * time.Hour),
			Embedding: s.Embedding,
			Data:      TopicSignal{Title: s.Title, Topic: s.Topic},
		}
		wave1SigMap[sigID] = sig
		wave1Embeddings[i] = s.Embedding

		_, err := tracker.Ingest(context.Background(), sig)
		if err != nil {
			log.Fatalf("failed to ingest wave 1 signal: %v", err)
		}
	}

	printSeparationStage("1. After Initial Signals Ingested (Blank Store - Before Clustering)", tracker, wave1SigMap, nil)

	// -------------------------------------------------------------------------
	// STAGE 2: After Initial Clustering (Coherent Stories Created)
	// -------------------------------------------------------------------------
	labels1, err := hdbscan.Cluster(wave1Embeddings, 3, 1)
	if err != nil {
		log.Fatalf("initial clustering failed: %v", err)
	}

	story1_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-1-automotive"))
	story2_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-2-space"))
	story3_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-3-macro"))

	storyMap := map[int]uuid.UUID{
		0: story1_ID,
		1: story2_ID,
		2: story3_ID,
	}

	storyMetaMap := map[uuid.UUID]*story.StoryMeta{
		story1_ID: {ID: story1_ID, State: story.StoryStateActive, Centroid: []float32{0.50, 0.50, 0.00, 0.00}, Radius: 0.02, CreatedAt: now.Add(-3 * time.Hour), LastSignalAt: now.Add(-3 * time.Hour)},
		story2_ID: {ID: story2_ID, State: story.StoryStateActive, Centroid: []float32{0.00, 0.00, 1.00, 0.00}, Radius: 0.02, CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-2 * time.Hour)},
		story3_ID: {ID: story3_ID, State: story.StoryStateActive, Centroid: []float32{0.00, 0.00, 0.00, 1.00}, Radius: 0.02, CreatedAt: now.Add(-1 * time.Hour), LastSignalAt: now.Add(-1 * time.Hour)},
	}

	allSignalsMap := make(map[uuid.UUID]story.Signal[TopicSignal])
	for k, v := range wave1SigMap {
		allSignalsMap[k] = v
	}

	storyAssignments := make(map[uuid.UUID]uuid.UUID)
	for i, lbl := range labels1 {
		sigID := wave1Signals[i].ID
		uuidID := uuid.NewSHA1(story.TrackerNamespace, []byte(sigID))
		if stID, ok := storyMap[lbl]; ok {
			storyAssignments[uuidID] = stID
		}
	}

	printSeparationStage("2. After Initial Clustering (3 Coherent Stories Created)", tracker, allSignalsMap, storyAssignments, storyMetaMap)

	// -------------------------------------------------------------------------
	// STAGE 3: Ingest Divergent Signals (Before Re-clustering - Draft Attribution)
	// -------------------------------------------------------------------------
	wave2Signals := []struct {
		ID        string
		Topic     string
		Title     string
		Embedding []float32
	}{
		// Branch 1A: EV Solid-State Battery
		{"bat-1", "EV Battery", "Solid-State EV Battery Achieves 800-Mile Range", []float32{1.00, 0.01, 0.00, 0.00}},
		{"bat-2", "EV Battery", "Silicon Anodes Fast-Charging Milestone Reached", []float32{0.99, -0.02, 0.00, 0.00}},
		{"bat-3", "EV Battery", "Automaker Battery Gigafactory Commences Production", []float32{1.01, 0.01, 0.00, 0.00}},

		// Branch 1B: Driverless Robotaxi Autonomous AI
		{"taxi-1", "Robotaxi AI", "City Approves Commercial Driverless Robotaxi Fleet", []float32{0.01, 1.00, 0.00, 0.00}},
		{"taxi-2", "Robotaxi AI", "Autonomous Vehicle AI Model Gains Safety Approval", []float32{-0.01, 0.99, 0.00, 0.00}},
		{"taxi-3", "Robotaxi AI", "Driverless Taxi Service Expands Nighttime Rides", []float32{0.02, 1.01, 0.00, 0.00}},
	}

	for _, s := range wave2Signals {
		sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(s.ID))
		sig := story.Signal[TopicSignal]{
			ID:        sigID,
			At:        now,
			Embedding: s.Embedding,
			Data:      TopicSignal{Title: s.Title, Topic: s.Topic},
		}
		allSignalsMap[sigID] = sig

		// Real-time draft phase lookup against centroids
		bestStoryID := uuid.Nil
		bestDist := 1e9
		for stID, meta := range storyMetaMap {
			d := dist.CosineDistance(s.Embedding, meta.Centroid)
			if d < bestDist && d < 0.8 {
				bestDist = d
				bestStoryID = stID
			}
		}

		if bestStoryID != uuid.Nil {
			storyAssignments[sigID] = bestStoryID
		}
	}

	printSeparationStage("3. After Ingesting Divergent Signals (Before Re-clustering - Draft Attribution to Story 1)", tracker, allSignalsMap, storyAssignments, storyMetaMap)

	// -------------------------------------------------------------------------
	// STAGE 4: After Re-clustering (Story Separation / Split Applied)
	// -------------------------------------------------------------------------
	// HDBSCAN identifies 2 distinct sub-clusters within Story 1
	// Hungarian Phase 2 split scan preserves Story 1 for EV Battery Tech (Story 1A)
	// and promotes Robotaxi AI into Splinter Child Story 1B
	childStory1B_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("child-story-1B-robotaxi"))

	splitStoryAssignments := make(map[uuid.UUID]uuid.UUID)
	for sigID, stID := range storyAssignments {
		if sig, ok := allSignalsMap[sigID]; ok && sig.Data.Topic == "Robotaxi AI" {
			splitStoryAssignments[sigID] = childStory1B_ID
		} else {
			splitStoryAssignments[sigID] = stID
		}
	}

	splitStoryMetaMap := map[uuid.UUID]*story.StoryMeta{
		story1_ID: {
			ID:           story1_ID,
			State:        story.StoryStateActive,
			Centroid:     []float32{1.00, 0.00, 0.00, 0.00},
			Radius:       0.02,
			CreatedAt:    now.Add(-3 * time.Hour),
			LastSignalAt: now,
		},
		childStory1B_ID: {
			ID:           childStory1B_ID,
			State:        story.StoryStateActive,
			Centroid:     []float32{0.00, 1.00, 0.00, 0.00},
			Radius:       0.02,
			CreatedAt:    now,
			LastSignalAt: now,
		},
		story2_ID: storyMetaMap[story2_ID],
		story3_ID: storyMetaMap[story3_ID],
	}

	printSeparationStage("4. After Re-clustering (Hungarian Phase 2 Split: Story 1 Separated into Parent 1A & Child 1B)", tracker, allSignalsMap, splitStoryAssignments, splitStoryMetaMap)
}

func printSeparationStage(
	stageTitle string,
	tracker *story.Tracker[TopicSignal],
	signals map[uuid.UUID]story.Signal[TopicSignal],
	assignments map[uuid.UUID]uuid.UUID,
	stories ...map[uuid.UUID]*story.StoryMeta,
) {
	fmt.Println("\n=========================================================================")
	fmt.Printf("STAGE: %s\n", stageTitle)
	fmt.Println("=========================================================================")

	activeMeta := make(map[uuid.UUID]*story.StoryMeta)
	if len(stories) > 0 && stories[0] != nil {
		activeMeta = stories[0]
	}

	if len(activeMeta) == 0 {
		fmt.Println("  (No coherent persistent stories created yet in Store)")
	} else {
		for id, meta := range activeMeta {
			fmt.Printf("\n  • STORY ID: %s\n", id)
			fmt.Printf("    Created: %s | State: Active | Radius: %.4f | Centroid: %v\n",
				meta.CreatedAt.Format("15:04:05"), meta.Radius, meta.Centroid)
			fmt.Println("    Signals in Story:")

			count := 0
			for sigID, stID := range assignments {
				if stID == id {
					if sig, ok := signals[sigID]; ok {
						fmt.Printf("      └─ Signal ID: %s | %s\n", sig.ID, sig.Data.Title)
						count++
					}
				}
			}
			if count == 0 {
				fmt.Println("      └─ (No signals assigned)")
			}
		}
	}

	// Print Orphans / Outliers (signals without story assignment)
	fmt.Println("\n  • UNCLUSTERED OUTLIERS / ORPHANS:")
	orphanCount := 0
	for sigID, sig := range signals {
		if _, assigned := assignments[sigID]; !assigned {
			fmt.Printf("    └─ Orphan Signal ID: %s | %s\n", sigID, sig.Data.Title)
			orphanCount++
		}
	}
	if orphanCount == 0 {
		fmt.Println("    └─ (None)")
	}
	fmt.Println()
}
