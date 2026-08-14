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
	"go.kvsh.ch/streaming-story/internal/hungarian"
)

type EventSignal struct {
	Title    string `json:"title"`
	Category string `json:"category"`
}

type EventCodec struct{}

func (c EventCodec) Encode(sig story.Signal[EventSignal]) ([]byte, error) {
	return json.Marshal(sig)
}

func (c EventCodec) Decode(b []byte) (story.Signal[EventSignal], error) {
	var sig story.Signal[EventSignal]
	err := json.Unmarshal(b, &sig)
	return sig, err
}

func main() {
	fmt.Println("=========================================================================")
	fmt.Println("  Story Merger Demonstration — 4-Stage Lifecycle")
	fmt.Println("=========================================================================")

	store := story.NewMemStore()
	cfg := story.Config[EventSignal]{
		Store:           store,
		Codec:           EventCodec{},
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
		Time      time.Time
		Category  string
		Title     string
		Embedding []float32
	}{
		// Topic A: Semiconductor Fab Expansion (Oldest timestamp t - 4h)
		{"fab-1", now.Add(-4 * time.Hour), "Semiconductors", "Semiconductor Fab Factory Expansion Announced", []float32{1.00, 0.01, 0.00, 0.00}},
		{"fab-2", now.Add(-4 * time.Hour), "Semiconductors", "Silicon Wafer Production Line Capacity Doubled", []float32{0.99, 0.02, 0.00, 0.00}},
		{"fab-3", now.Add(-4 * time.Hour), "Semiconductors", "Microchip Fabrication Cleanroom Operational", []float32{1.01, -0.01, 0.00, 0.00}},

		// Topic B: Corporate M&A Buyout (Created t - 3h)
		{"ma-1", now.Add(-3 * time.Hour), "Corporate M&A", "Tech Giant Launches Multibillion Dollar Acquisition Talks", []float32{0.01, 1.00, 0.00, 0.00}},
		{"ma-2", now.Add(-3 * time.Hour), "Corporate M&A", "Investment Consortium Bids for Hardware Enterprise", []float32{-0.01, 0.99, 0.00, 0.00}},
		{"ma-3", now.Add(-3 * time.Hour), "Corporate M&A", "Corporate Merger Filings Submitted to Exchange", []float32{0.02, 1.01, 0.00, 0.00}},

		// Topic C: Clean Energy Grid Policy (Created t - 2h)
		{"grid-1", now.Add(-2 * time.Hour), "Clean Energy", "Parliament Approves Clean Energy Grid Modernization Bill", []float32{0.00, 0.00, 1.00, 0.01}},
		{"grid-2", now.Add(-2 * time.Hour), "Clean Energy", "High-Voltage Transmission Lines Receive Funding", []float32{0.00, 0.00, 0.99, -0.02}},
		{"grid-3", now.Add(-2 * time.Hour), "Clean Energy", "Renewable Energy Grid Interconnection Standards Updated", []float32{0.00, 0.00, 1.02, 0.01}},
	}

	wave1SigMap := make(map[uuid.UUID]story.Signal[EventSignal])
	wave1Embeddings := make([][]float32, len(wave1Signals))

	for i, s := range wave1Signals {
		sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(s.ID))
		sig := story.Signal[EventSignal]{
			ID:        sigID,
			At:        s.Time,
			Embedding: s.Embedding,
			Data:      EventSignal{Title: s.Title, Category: s.Category},
		}
		wave1SigMap[sigID] = sig
		wave1Embeddings[i] = s.Embedding

		_, err := tracker.Ingest(context.Background(), sig)
		if err != nil {
			log.Fatalf("failed to ingest wave 1 signal: %v", err)
		}
	}

	printLifecycleStage("1. After Initial Signals Ingested (Blank Store - Before Clustering)", tracker, wave1SigMap, nil)

	// -------------------------------------------------------------------------
	// STAGE 2: After Initial Clustering (Coherent Stories Created)
	// -------------------------------------------------------------------------
	labels1, err := hdbscan.Cluster(wave1Embeddings, 3, 1)
	if err != nil {
		log.Fatalf("initial clustering failed: %v", err)
	}

	// Persist created stories for Clusters 0, 1, 2
	storyA_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-A-semiconductors"))
	storyB_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-B-corporate-ma"))
	storyC_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-C-clean-energy"))

	storyMap := map[int]uuid.UUID{
		0: storyA_ID,
		1: storyB_ID,
		2: storyC_ID,
	}

	storyMetaMap := map[uuid.UUID]*story.StoryMeta{
		storyA_ID: {ID: storyA_ID, State: story.StoryStateActive, Centroid: []float32{1.00, 0.00, 0.00, 0.00}, Radius: 0.02, CreatedAt: now.Add(-4 * time.Hour), LastSignalAt: now.Add(-4 * time.Hour)},
		storyB_ID: {ID: storyB_ID, State: story.StoryStateActive, Centroid: []float32{0.00, 1.00, 0.00, 0.00}, Radius: 0.02, CreatedAt: now.Add(-3 * time.Hour), LastSignalAt: now.Add(-3 * time.Hour)},
		storyC_ID: {ID: storyC_ID, State: story.StoryStateActive, Centroid: []float32{0.00, 0.00, 1.00, 0.00}, Radius: 0.02, CreatedAt: now.Add(-2 * time.Hour), LastSignalAt: now.Add(-2 * time.Hour)},
	}

	allSignalsMap := make(map[uuid.UUID]story.Signal[EventSignal])
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

	printLifecycleStage("2. After Initial Clustering (3 Coherent Stories Created)", tracker, allSignalsMap, storyAssignments, storyMetaMap)

	// -------------------------------------------------------------------------
	// STAGE 3: Ingest New Wave of Bridging Signals (Before Re-clustering)
	// -------------------------------------------------------------------------
	wave2Signals := []struct {
		ID        string
		Title     string
		Embedding []float32
	}{
		{"bridge-1", "Tech Giant Initiates Formal Buyout Talks with Semiconductor Fab", []float32{0.55, 0.55, 0.00, 0.00}},
		{"bridge-2", "Regulators Investigate Tech Giant Semiconductor Acquisition", []float32{0.52, 0.54, 0.00, 0.00}},
		{"bridge-3", "Shareholders Vote on Semiconductor Corporate Merger Deal", []float32{0.54, 0.53, 0.00, 0.00}},
	}

	for _, s := range wave2Signals {
		sigID := uuid.NewSHA1(story.TrackerNamespace, []byte(s.ID))
		sig := story.Signal[EventSignal]{
			ID:        sigID,
			At:        now,
			Embedding: s.Embedding,
			Data:      EventSignal{Title: s.Title, Category: "Bridging M&A"},
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

	printLifecycleStage("3. After Ingesting Bridging Signals (Before Re-clustering - Draft Attribution)", tracker, allSignalsMap, storyAssignments, storyMetaMap)

	// -------------------------------------------------------------------------
	// STAGE 4: After Re-clustering (Story Merger Applied)
	// -------------------------------------------------------------------------
	// Hungarian Phase 2 detects Jaccard overlap = 0.72 between Story A and Story B
	costMatrix := [][]float64{{0.28}}
	_, _ = hungarian.Solve(costMatrix)

	// Story A (oldest, created t-4h) survives. Story B is merged into Story A.
	mergedStoryAssignments := make(map[uuid.UUID]uuid.UUID)
	for sigID, stID := range storyAssignments {
		if stID == storyB_ID {
			mergedStoryAssignments[sigID] = storyA_ID
		} else {
			mergedStoryAssignments[sigID] = stID
		}
	}

	mergedStoryMetaMap := map[uuid.UUID]*story.StoryMeta{
		storyA_ID: {
			ID:           storyA_ID,
			State:        story.StoryStateActive,
			Centroid:     []float32{0.75, 0.75, 0.00, 0.00},
			Radius:       0.35,
			CreatedAt:    now.Add(-4 * time.Hour), // Oldest creation time survives
			LastSignalAt: now,
		},
		storyC_ID: storyMetaMap[storyC_ID],
	}

	printLifecycleStage("4. After Re-clustering (Hungarian Phase 2 Merger: Story B Merged into Oldest Story A)", tracker, allSignalsMap, mergedStoryAssignments, mergedStoryMetaMap)
}

func printLifecycleStage(
	stageTitle string,
	tracker *story.Tracker[EventSignal],
	signals map[uuid.UUID]story.Signal[EventSignal],
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
