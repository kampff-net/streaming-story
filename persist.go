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
type storyRecord struct {
	State              StoryState `json:"state"`
	Centroid           []float32  `json:"centroid"`
	Radius             float64    `json:"radius"`
	CreatedAt          time.Time  `json:"created_at"`
	LastSignalAt       time.Time  `json:"last_signal_at"`
	MeanDistance       float64    `json:"mean_distance,omitempty"`
	Sigma              float64    `json:"sigma,omitempty"`
	SignalCount        int        `json:"signal_count,omitempty"`
	FrozenMeanDistance float64    `json:"frozen_mean_distance,omitempty"`
	FrozenSigma        float64    `json:"frozen_sigma,omitempty"`
}
