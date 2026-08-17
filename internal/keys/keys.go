package keys

import (
	"bytes"
	"fmt"

	"github.com/google/uuid"
)

// Package keys is the KV key schema: every byte layout the store uses, and the
// parsers that read them back. It is kept apart from the tracker so the schema
// can be reasoned about — and tested — as one thing, and so no other file
// hand-assembles a key.
//
// Key schema (all keys are ASCII, lexicographically orderable):
//
//   c:state                        — calibrator state (σ_global, dimensionality, last batch)
//   s:{storyID}:m                  — story metadata
//   s:{storyID}:s:{signalID}       — signal data belonging to a story
//   o:{signalID}                   — outlier signal (not yet assigned to a story)
//   l:{signalID}                   — signal location: the story it belongs to, or "o" for the outlier bucket
//   t:{unix_sec_10digits}:{storyID} — time index for Tier 3 range scans

func CalibState() []byte {
	return []byte("c:state")
}

func StoryMeta(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:m", storyID)
}

// StoryPrefix returns the prefix covering all keys for a story
// (metadata + signals): "s:{storyID}:".
func StoryPrefix(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:", storyID)
}

func Signal(storyID, signalID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:s:%s", storyID, signalID)
}

// SignalPrefix returns the prefix covering all signal keys for a story:
// "s:{storyID}:s:".
func SignalPrefix(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:s:", storyID)
}

func Outlier(signalID uuid.UUID) []byte {
	return fmt.Appendf(nil, "o:%s", signalID)
}

// SignalLoc returns the location-index key for a signal. The value is
// "s:{storyID}" when the signal belongs to a story or "o" when it is in the
// outlier bucket. It lets Ingest find where a signal currently lives so
// re-ingestion never duplicates a copy that a batch run moved.
func SignalLoc(signalID uuid.UUID) []byte {
	return fmt.Appendf(nil, "l:%s", signalID)
}

// TimeIndex returns a time-index key for the given story.
// unixSec is zero-padded to 10 digits (sufficient until year 2286) so keys
// sort lexicographically by time.
func TimeIndex(unixSec int64, storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "t:%010d:%s", unixSec, storyID)
}

// TimeIndexFrom returns the lower bound for a range scan starting at unixSec.
func TimeIndexFrom(unixSec int64) []byte {
	return fmt.Appendf(nil, "t:%010d:", unixSec)
}

// ParseTimeIndex extracts the story ID from a "t:{unix_sec}:{storyID}"
// time-index key. It returns ok=false for keys that do not match the shape.
func ParseTimeIndex(key []byte) (uuid.UUID, bool) {
	if len(key) < len("t::") || key[0] != 't' || key[1] != ':' {
		return uuid.Nil, false
	}
	rest := key[2:]
	colon := bytes.IndexByte(rest, ':')
	if colon < 0 {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(string(rest[colon+1:]))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// ParseSignalIDFromLoc extracts the signal ID from a signal location key,
// which is either "o:{signalID}" (outlier bucket) or "s:{storyID}:s:{signalID}".
func ParseSignalIDFromLoc(key []byte) (uuid.UUID, bool) {
	if len(key) < len("o:") || key[1] != ':' {
		return uuid.Nil, false
	}
	if key[0] == 'o' {
		id, err := uuid.Parse(string(key[2:]))
		if err != nil {
			return uuid.Nil, false
		}
		return id, true
	}
	if key[0] == 's' {
		rest := key[2:]
		colon := bytes.IndexByte(rest, ':')
		if colon < 0 || len(rest) < colon+3 || rest[colon+1] != 's' || rest[colon+2] != ':' {
			return uuid.Nil, false
		}
		id, err := uuid.Parse(string(rest[colon+3:]))
		if err != nil {
			return uuid.Nil, false
		}
		return id, true
	}
	return uuid.Nil, false
}

// IsOutlier reports whether key is an "o:{signalID}" outlier key.
func IsOutlier(key []byte) bool {
	return len(key) > len("o:") && key[0] == 'o' && key[1] == ':'
}

// ParseStoryIDFromSignal extracts the story ID from a
// "s:{storyID}:s:{signalID}" signal key.
func ParseStoryIDFromSignal(key []byte) (uuid.UUID, bool) {
	if len(key) < len("s::s:") || key[0] != 's' || key[1] != ':' {
		return uuid.Nil, false
	}
	rest := key[2:]
	colon := bytes.IndexByte(rest, ':')
	if colon < 0 {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(string(rest[:colon]))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// ParseStoryMeta extracts the story ID from a "s:{storyID}:m" metadata key.
// It returns ok=false for keys that do not match the metadata key shape
// (for example signal keys, which share the "s:" prefix).
func ParseStoryMeta(key []byte) (uuid.UUID, bool) {
	if len(key) < len("s::m") || string(key[len(key)-2:]) != ":m" {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(string(key[2 : len(key)-2]))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// ParseSignal extracts the signal ID from a "s:{storyID}:s:{signalID}" key
// given the key's prefix.
func ParseSignal(key, prefix []byte) (uuid.UUID, bool) {
	if len(key) <= len(prefix) {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(string(key[len(prefix):]))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// EncodeSignalLoc builds a location-index value: "s:{storyID}" for story
// membership, "o" for the outlier bucket.
func EncodeSignalLoc(storyID uuid.UUID, isOutlier bool) []byte {
	if isOutlier {
		return []byte("o")
	}
	return fmt.Appendf(nil, "s:%s", storyID)
}

// ParseSignalLoc decodes a location-index value written by EncodeSignalLoc.
func ParseSignalLoc(val []byte) (storyID uuid.UUID, isOutlier, ok bool) {
	if len(val) == 1 && val[0] == 'o' {
		return uuid.Nil, true, true
	}
	if len(val) > 2 && val[0] == 's' && val[1] == ':' {
		id, err := uuid.Parse(string(val[2:]))
		if err != nil {
			return uuid.Nil, false, false
		}
		return id, false, true
	}
	return uuid.Nil, false, false
}
