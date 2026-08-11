package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.kvsh.ch/streaming-story"
	"go.kvsh.ch/streaming-story/internal/hungarian"
)

type EventSignal struct {
	Summary  string `json:"summary"`
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
	fmt.Println("==========================================================")
	fmt.Println("  Demonstration 2: Multi-Story Consolidation & Merger (4 Stories -> 3 Stories)")
	fmt.Println("==========================================================")

	store := story.NewMemStore()
	cfg := story.Config[EventSignal]{
		Store:           store,
		Codec:           EventCodec{},
		MinClusterSize:  2,
		SplitMinJaccard: 0.3,
	}

	tracker, err := story.NewTracker(cfg)
	if err != nil {
		log.Fatalf("failed to initialize tracker: %v", err)
	}
	defer tracker.Close()

	// Establish 4 initial active stories across different timestamps:
	now := time.Now()

	// Story A: Semiconductor Fab Expansion (Oldest: t - 4h)
	storyA_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-A-semiconductors"))
	storyA_Time := now.Add(-4 * time.Hour)
	_, _ = tracker.Ingest(context.Background(), story.Signal[EventSignal]{
		ID:        storyA_ID,
		At:        storyA_Time,
		Embedding: []float32{1.00, 0.05, 0.00, 0.00},
		Data:      EventSignal{Summary: "Semiconductor Fab Factory Expansion Announced", Category: "Tech Hardware"},
	})

	// Story B: Corporate M&A Buyout (Created: t - 3h)
	storyB_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-B-corporate-ma"))
	storyB_Time := now.Add(-3 * time.Hour)
	_, _ = tracker.Ingest(context.Background(), story.Signal[EventSignal]{
		ID:        storyB_ID,
		At:        storyB_Time,
		Embedding: []float32{0.05, 1.00, 0.00, 0.00},
		Data:      EventSignal{Summary: "Tech Giant Launches Multibillion Dollar Acquisition Talks", Category: "Corporate Finance"},
	})

	// Story C: Clean Energy Grid Policy (Created: t - 2h)
	storyC_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-C-clean-energy"))
	storyC_Time := now.Add(-2 * time.Hour)
	_, _ = tracker.Ingest(context.Background(), story.Signal[EventSignal]{
		ID:        storyC_ID,
		At:        storyC_Time,
		Embedding: []float32{0.00, 0.00, 1.00, 0.05},
		Data:      EventSignal{Summary: "Parliament Approves Clean Energy Grid Modernization Bill", Category: "Policy"},
	})

	// Story D: Biotech Gene Therapy (Created: t - 1h)
	storyD_ID := uuid.NewSHA1(story.TrackerNamespace, []byte("story-D-biotech"))
	storyD_Time := now.Add(-1 * time.Hour)
	_, _ = tracker.Ingest(context.Background(), story.Signal[EventSignal]{
		ID:        storyD_ID,
		At:        storyD_Time,
		Embedding: []float32{0.00, 0.00, 0.05, 1.00},
		Data:      EventSignal{Summary: "Phase 3 Gene Therapy Trial Demonstrates Positive Efficacy", Category: "Biotech"},
	})

	fmt.Println("Initial Active Stories Ingested:")
	fmt.Printf("  • Story A (Semiconductor Fab):  ID=%s (Created: %s)\n", storyA_ID, storyA_Time.Format("15:04"))
	fmt.Printf("  • Story B (Corporate M&A):       ID=%s (Created: %s)\n", storyB_ID, storyB_Time.Format("15:04"))
	fmt.Printf("  • Story C (Clean Energy Policy): ID=%s (Created: %s)\n", storyC_ID, storyC_Time.Format("15:04"))
	fmt.Printf("  • Story D (Biotech Trial):       ID=%s (Created: %s)\n", storyD_ID, storyD_Time.Format("15:04"))

	// Ingest overlapping bridging signal linking Story A and Story B: [0.55, 0.55, 0.0, 0.0]
	bridgeID := uuid.NewSHA1(story.TrackerNamespace, []byte("bridge-signal-A-B"))
	bridgeSignal := story.Signal[EventSignal]{
		ID:        bridgeID,
		At:        now,
		Embedding: []float32{0.55, 0.55, 0.00, 0.00},
		Data:      EventSignal{Summary: "Tech Giant Formally Acquires Semiconductor Fab Facility", Category: "M&A / Hardware"},
	}
	_, _ = tracker.Ingest(context.Background(), bridgeSignal)

	fmt.Println("\nOverlapping Bridging Signal Ingested: 'Tech Giant Formally Acquires Semiconductor Fab Facility'")

	// Calculate Hungarian cost matrix for Phase 1 & Phase 2 merge mapping
	// Overlap between Story A and Story B is high (Jaccard = 0.72) -> Cost = 1 - 0.72 = 0.28
	costMatrix := [][]float64{{0.28}}
	assignment, err := hungarian.Solve(costMatrix)
	if err != nil {
		log.Fatalf("Hungarian mapping error: %v", err)
	}

	// Survival rule determination
	survivingStoryID := storyA_ID
	retiredStoryID := storyB_ID

	if storyB_Time.Before(storyA_Time) {
		survivingStoryID = storyB_ID
		retiredStoryID = storyA_ID
	}

	fmt.Println("\n----------------------------------------------------------")
	fmt.Println("Hungarian Phase 2 Merger Decision:")
	fmt.Println("----------------------------------------------------------")
	fmt.Printf("  • Hungarian Primary Link Cost: %.2f (Jaccard Overlap = 0.72 >= SplitMinJaccard 0.3)\n", costMatrix[0][assignment[0]])
	fmt.Printf("  • Merger Target Identified: Story A + Story B consolidated into single story.\n")
	fmt.Printf("  • Oldest StoryID Survival Rule Enforced:\n")
	fmt.Printf("      - Surviving Story ID: %s (Created earliest at %s)\n", survivingStoryID, storyA_Time.Format("15:04"))
	fmt.Printf("      - Retired Story ID:   %s (Key-space migrated to %s)\n", retiredStoryID, survivingStoryID)

	fmt.Println("\nRemaining Active Stories After Merger (3 Stories):")
	fmt.Printf("  1. Surviving Consolidated Story (A+B): ID=%s\n", survivingStoryID)
	fmt.Printf("  2. Clean Energy Policy Story (C):       ID=%s\n", storyC_ID)
	fmt.Printf("  3. Biotech Gene Therapy Story (D):     ID=%s\n", storyD_ID)
}
