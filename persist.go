package story

import "time"

// calibState is the JSON-serialised form of the global calibration state
// stored at keyCalibState().
type calibState struct {
	SigmaGlobal float64   `json:"sigma_global"`
	Dim         int       `json:"dim"`
	LastBatchAt time.Time `json:"last_batch_at"`
}

// storyRecord is the JSON-serialised form of story metadata stored at
// keyStoryMeta(). It mirrors StoryMeta but keeps JSON tags out of the
// public type.
// recentOrCentroid returns the recency centroid, falling back to the lifetime
// centroid for stories with no members inside ActiveContextWindow — every
// Dormant story, and any Active one whose recent traffic has lapsed.
func recentOrCentroid(rec storyRecord) []float32 {
	if len(rec.RecentCentroid) > 0 {
		return rec.RecentCentroid
	}
	return rec.Centroid
}

type storyRecord struct {
	State              StoryState `json:"state"`
	Centroid           []float32  `json:"centroid"`
	RecentCentroid     []float32  `json:"recent_centroid,omitempty"`
	Radius             float64    `json:"radius"`
	CreatedAt          time.Time  `json:"created_at"`
	LastSignalAt       time.Time  `json:"last_signal_at"`
	MeanDistance       float64    `json:"mean_distance,omitempty"`
	Sigma              float64    `json:"sigma,omitempty"`
	SignalCount        int        `json:"signal_count,omitempty"`
	FrozenMeanDistance float64    `json:"frozen_mean_distance,omitempty"`
	FrozenSigma        float64    `json:"frozen_sigma,omitempty"`
}
