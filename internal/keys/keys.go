package keys

import (
	"bytes"
	"fmt"

	"github.com/fxamacker/cbor/v2"
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
//   g:{signalID}                   — canonical signal record: the one authoritative copy
//   s:{storyID}:f:{signalID}:{facet} — facet membership marker, payload-free
//   o:{signalID}:{facet}           — unplaced facet marker, payload-free
//   l:{signalID}                   — signal location: one entry per facet
//   t:{unix_sec_10digits}:{storyID} — time index for Tier 3 range scans

func CalibState() []byte {
	return []byte("c:state")
}

func StoryMeta(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:m", storyID)
}

func StoryHot(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:h", storyID)
}

// StoryPrefix returns the prefix covering all keys for a story
// (metadata + signals): "s:{storyID}:".
func StoryPrefix(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:", storyID)
}

// SignalLoc returns the location-index key for a signal. The value is the
// per-facet location set written by EncodeSignalLocSet. It lets Ingest find
// where a signal's facets currently live so re-ingestion never duplicates a
// placement a batch run made.
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

// ParseStoryHot extracts the story ID from a "s:{storyID}:h" hot state key.
func ParseStoryHot(key []byte) (uuid.UUID, bool) {
	if len(key) < len("s::h") || string(key[len(key)-2:]) != ":h" {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(string(key[2 : len(key)-2]))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// --- spec 007: facet-granular schema ---
//
// The signal payload moves out from under its owning story into a canonical
// record, so a signal that belongs to several stories is stored once. Membership
// becomes a payload-free marker per facet.
//
//   g:{signalID}                       — canonical signal record, one copy
//   s:{storyID}:f:{signalID}:{facet}   — facet membership marker
//   o:{signalID}:{facet}               — unplaced facet marker
//
// {facet} is zero-padded to four digits so a signal's facet keys sort in facet
// order, which bounds Config.MaxFacetsPerSignal at 9999.

// facetDigits is the zero-padded width of a facet index in a key.
const facetDigits = 4

// MaxFacet is the largest facet index the key schema can represent.
const MaxFacet = 9999

// CanonicalSignal returns the key holding the one authoritative copy of a
// signal, independent of where its facets are placed.
func CanonicalSignal(signalID uuid.UUID) []byte {
	return fmt.Appendf(nil, "g:%s", signalID)
}

// CanonicalPrefix returns the prefix covering every canonical signal record.
func CanonicalPrefix() []byte {
	return []byte("g:")
}

// ParseCanonicalSignal extracts the signal ID from a "g:{signalID}" key.
func ParseCanonicalSignal(key []byte) (uuid.UUID, bool) {
	if len(key) < len("g:") || key[0] != 'g' || key[1] != ':' {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(string(key[2:]))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// FacetMember returns the membership marker key for one facet of one signal
// under a story. The value carries no payload; the signal lives at
// CanonicalSignal.
func FacetMember(storyID, signalID uuid.UUID, facet int) []byte {
	return fmt.Appendf(nil, "s:%s:f:%s:%0*d", storyID, signalID, facetDigits, facet)
}

// FacetPrefix returns the prefix covering all facet membership markers for a
// story: "s:{storyID}:f:".
func FacetPrefix(storyID uuid.UUID) []byte {
	return fmt.Appendf(nil, "s:%s:f:", storyID)
}

// OutlierFacet returns the key for an unplaced facet.
func OutlierFacet(signalID uuid.UUID, facet int) []byte {
	return fmt.Appendf(nil, "o:%s:%0*d", signalID, facetDigits, facet)
}

// OutlierPrefix returns the prefix covering every unplaced facet.
func OutlierPrefix() []byte {
	return []byte("o:")
}

// OutlierSignalPrefix returns the prefix covering every unplaced facet of one
// signal: "o:{signalID}:". It is how a caller enumerates a signal's outlier
// markers without trusting the location index, which is derived state and may
// be shorter than the set of markers actually present.
func OutlierSignalPrefix(signalID uuid.UUID) []byte {
	return fmt.Appendf(nil, "o:%s:", signalID)
}

// parseIDFacet reads a "{signalID}:{facet}" tail. It is the shared tail format
// of both a facet membership marker and an outlier facet key.
func parseIDFacet(tail []byte) (uuid.UUID, int, bool) {
	colon := bytes.LastIndexByte(tail, ':')
	if colon < 0 {
		return uuid.Nil, 0, false
	}
	id, err := uuid.Parse(string(tail[:colon]))
	if err != nil {
		return uuid.Nil, 0, false
	}
	digits := tail[colon+1:]
	// Fixed width, digits only: a short or malformed index must be rejected
	// rather than silently parsed as facet 0.
	if len(digits) != facetDigits {
		return uuid.Nil, 0, false
	}
	facet := 0
	for _, c := range digits {
		if c < '0' || c > '9' {
			return uuid.Nil, 0, false
		}
		facet = facet*10 + int(c-'0')
	}
	return id, facet, true
}

// ParseFacetMember extracts the signal ID and facet index from a facet
// membership key, given the key's prefix.
func ParseFacetMember(key, prefix []byte) (uuid.UUID, int, bool) {
	if len(key) <= len(prefix) || !bytes.HasPrefix(key, prefix) {
		return uuid.Nil, 0, false
	}
	return parseIDFacet(key[len(prefix):])
}

// ParseOutlierFacet extracts the signal ID and facet index from an
// "o:{signalID}:{facet}" key.
func ParseOutlierFacet(key []byte) (uuid.UUID, int, bool) {
	if len(key) < len("o:") || key[0] != 'o' || key[1] != ':' {
		return uuid.Nil, 0, false
	}
	return parseIDFacet(key[2:])
}

// FacetLoc is where one facet of a signal currently lives. The zero value —
// no story, not an outlier — means the facet has no location, which happens
// only for a signal whose record exists but whose placement has not been
// written yet.
type FacetLoc struct {
	StoryID   uuid.UUID `cbor:"0,keyasint,omitempty"`
	IsOutlier bool      `cbor:"1,keyasint,omitempty"`
}

var (
	cborEncMode = mustEncMode()
	cborStrictDecMode = mustDecMode()
)

func mustEncMode() cbor.EncMode {
	opts := cbor.CanonicalEncOptions()
	opts.Time = cbor.TimeRFC3339Nano
	opts.TimeTag = cbor.EncTagNone
	m, err := opts.EncMode()
	if err != nil {
		panic("keys: cbor enc mode: " + err.Error())
	}
	return m
}

func mustDecMode() cbor.DecMode {
	opts := cbor.DecOptions{
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
	}
	m, err := opts.DecMode()
	if err != nil {
		panic("keys: cbor dec mode: " + err.Error())
	}
	return m
}

// EncodeSignalLocSet builds the location-index value for a whole signal: one
// entry per facet, in facet order.
//
// The index is derived state. It is rebuildable in full from the facet
// membership and outlier key spaces, and is kept only so Ingest can find where
// a signal lives without scanning them.
func EncodeSignalLocSet(locs []FacetLoc) []byte {
	b, err := cborEncMode.Marshal(locs)
	if err != nil {
		return nil
	}
	return b
}

// ParseSignalLocSet decodes a location-index value written by
// EncodeSignalLocSet.
func ParseSignalLocSet(val []byte) ([]FacetLoc, bool) {
	var locs []FacetLoc
	if err := cborStrictDecMode.Unmarshal(val, &locs); err != nil {
		return nil, false
	}
	return locs, true
}
