package story

import (
	"bytes"
	"fmt"

	"github.com/google/uuid"
)

// Key schema (all keys are ASCII, lexicographically orderable):
//
//   c:state                        — calibrator state (σ_global, dimensionality, last batch)
//   s:{storyID}:m                  — story metadata
//   s:{storyID}:s:{signalID}       — signal data belonging to a story
//   o:{signalID}                   — outlier signal (not yet assigned to a story)
//   l:{signalID}                   — signal location: the story it belongs to, or "o" for the outlier bucket
//   t:{unix_sec_10digits}:{storyID} — time index for Tier 3 range scans

func keyCalibState() []byte {
	return []byte("c:state")
}

func keyStoryMeta(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:m", storyID)
}

// keyStoryPrefix returns the prefix covering all keys for a story
// (metadata + signals): "s:{storyID}:".
func keyStoryPrefix(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:", storyID)
}

func keySignal(storyID, signalID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:s:%s", storyID, signalID)
}

// keySignalPrefix returns the prefix covering all signal keys for a story:
// "s:{storyID}:s:".
func keySignalPrefix(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:s:", storyID)
}

func keyOutlier(signalID uuid.UUID) []byte {
	return fmt.Appendf(nil, "o:%s", signalID)
}

// keySignalLoc returns the location-index key for a signal. The value is
// "s:{storyID}" when the signal belongs to a story or "o" when it is in the
// outlier bucket. It lets Ingest find where a signal currently lives so
// re-ingestion never duplicates a copy that a batch run moved.
func keySignalLoc(signalID uuid.UUID) []byte {
	return fmt.Appendf(nil, "l:%s", signalID)
}

// keyTimeIndex returns a time-index key for the given story.
// unixSec is zero-padded to 10 digits (sufficient until year 2286) so keys
// sort lexicographically by time.
func keyTimeIndex(unixSec int64, storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "t:%010d:%s", unixSec, storyID)
}

// keyTimeIndexFrom returns the lower bound for a range scan starting at unixSec.
func keyTimeIndexFrom(unixSec int64) []byte {
	return fmt.Appendf(nil, "t:%010d:", unixSec)
}

// parseTimeIndexKey extracts the story ID from a "t:{unix_sec}:{storyID}"
// time-index key. It returns ok=false for keys that do not match the shape.
func parseTimeIndexKey(key []byte) (uuid.UUID, bool) {
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

// parseSignalIDFromLocKey extracts the signal ID from a signal location key,
// which is either "o:{signalID}" (outlier bucket) or "s:{storyID}:s:{signalID}".
func parseSignalIDFromLocKey(key []byte) (uuid.UUID, bool) {
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

// isOutlierKey reports whether key is an "o:{signalID}" outlier key.
func isOutlierKey(key []byte) bool {
	return len(key) > len("o:") && key[0] == 'o' && key[1] == ':'
}

// parseStoryIDFromSignalKey extracts the story ID from a
// "s:{storyID}:s:{signalID}" signal key.
func parseStoryIDFromSignalKey(key []byte) (uuid.UUID, bool) {
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
