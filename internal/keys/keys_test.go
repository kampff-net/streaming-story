package keys

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	keysTestStoryID  = uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	keysTestSignalID = uuid.MustParse("11111111-2222-3333-4444-555555555555")
)

func TestKeyCalibState(t *testing.T) {
	assert.Equal(t, []byte("c:state"), CalibState())
}

func TestKeyStoryMeta(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:m"),
		StoryMeta(keysTestStoryID),
	)
	id, ok := ParseStoryMeta(StoryMeta(keysTestStoryID))
	require.True(t, ok)
	assert.Equal(t, keysTestStoryID, id)
}

func TestKeyStoryHot(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:h"),
		StoryHot(keysTestStoryID),
	)
	id, ok := ParseStoryHot(StoryHot(keysTestStoryID))
	require.True(t, ok)
	assert.Equal(t, keysTestStoryID, id)
}

func TestKeyStoryPrefix(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:"),
		StoryPrefix(keysTestStoryID),
	)
}

func TestKeySignalLoc(t *testing.T) {
	assert.Equal(t,
		[]byte("l:11111111-2222-3333-4444-555555555555"),
		SignalLoc(keysTestSignalID),
	)
}

func TestParseTimeIndexKey(t *testing.T) {
	id := uuid.New()
	key := TimeIndex(1_700_000_000, id)
	got, ok := ParseTimeIndex(key)
	require.True(t, ok)
	assert.Equal(t, id, got)

	_, ok = ParseTimeIndex([]byte("s:story:m"))
	assert.False(t, ok)
	_, ok = ParseTimeIndex([]byte("t:abc"))
	assert.False(t, ok)
	_, ok = ParseTimeIndex([]byte("t:"))
	assert.False(t, ok)
}

func TestKeyTimeIndex(t *testing.T) {
	t.Run("exact_format", func(t *testing.T) {
		assert.Equal(t,
			[]byte("t:1234567890:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			TimeIndex(1234567890, keysTestStoryID),
		)
	})

	t.Run("zero_pads_to_10_digits", func(t *testing.T) {
		assert.Equal(t,
			[]byte("t:0000000001:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			TimeIndex(1, keysTestStoryID),
		)
	})

	t.Run("lexicographic_order_matches_chronological_order", func(t *testing.T) {
		earlier := TimeIndex(1_000_000_000, keysTestStoryID)
		later := TimeIndex(2_000_000_000, keysTestStoryID)
		assert.Negative(t, bytes.Compare(earlier, later),
			"earlier timestamp must produce a key that sorts before later timestamp")
	})
}

func TestKeyTimeIndexFrom(t *testing.T) {
	assert.Equal(t, []byte("t:1234567890:"), TimeIndexFrom(1234567890))
}

// --- spec 007: facet-granular schema ---

func TestCanonicalSignal_RoundTrip(t *testing.T) {
	id := uuid.New()
	key := CanonicalSignal(id)
	assert.True(t, bytes.HasPrefix(key, CanonicalPrefix()))

	got, ok := ParseCanonicalSignal(key)
	require.True(t, ok)
	assert.Equal(t, id, got)
}

func TestCanonicalSignal_RejectsOtherSpaces(t *testing.T) {
	for _, key := range [][]byte{
		StoryMeta(uuid.New()),
		OutlierFacet(uuid.New(), 0),
		[]byte("g:not-a-uuid"),
		[]byte("g:"),
		nil,
	} {
		_, ok := ParseCanonicalSignal(key)
		assert.False(t, ok, "key %q must not parse as a canonical record", key)
	}
}

func TestFacetMember_RoundTrip(t *testing.T) {
	story, sig := uuid.New(), uuid.New()
	prefix := FacetPrefix(story)

	key := FacetMember(story, sig, 7)
	assert.True(t, bytes.HasPrefix(key, prefix))

	gotSig, gotFacet, ok := ParseFacetMember(key, prefix)
	require.True(t, ok)
	assert.Equal(t, sig, gotSig)
	assert.Equal(t, 7, gotFacet)
}

// The membership prefix must not collide with the metadata key: both live
// under "s:{storyID}:".
func TestFacetPrefix_DoesNotCollide(t *testing.T) {
	story := uuid.New()
	prefix := FacetPrefix(story)

	assert.False(t, bytes.HasPrefix(StoryMeta(story), prefix))
	assert.True(t, bytes.HasPrefix(prefix, StoryPrefix(story)), "facet keys live under the story prefix")
	assert.True(t, bytes.HasPrefix(FacetMember(story, uuid.New(), 0), prefix))
}

// Facet keys of one signal must sort in facet order, which is what the
// zero-padding buys and what an unpadded index would break at 9 -> 10.
func TestFacetMember_SortsByFacetIndex(t *testing.T) {
	story, sig := uuid.New(), uuid.New()
	prev := FacetMember(story, sig, 0)
	for facet := 1; facet <= MaxFacet; facet *= 3 {
		key := FacetMember(story, sig, facet)
		assert.Negative(t, bytes.Compare(prev, key), "facet %d must sort after its predecessor", facet)
		prev = key
	}
}

func TestOutlierFacet_RoundTrip(t *testing.T) {
	sig := uuid.New()
	key := OutlierFacet(sig, 3)
	assert.True(t, bytes.HasPrefix(key, OutlierPrefix()))

	gotSig, gotFacet, ok := ParseOutlierFacet(key)
	require.True(t, ok)
	assert.Equal(t, sig, gotSig)
	assert.Equal(t, 3, gotFacet)
}

// A malformed facet index must be rejected outright. Parsing it as facet 0
// would silently alias every broken key onto a real facet.
func TestParseFacet_RejectsMalformedIndex(t *testing.T) {
	sig := uuid.New()
	for _, key := range [][]byte{
		[]byte("o:" + sig.String() + ":12"),    // too short
		[]byte("o:" + sig.String() + ":00012"), // too long
		[]byte("o:" + sig.String() + ":00x1"),  // not digits
		[]byte("o:" + sig.String()),            // no index at all
		[]byte("o:" + sig.String() + ":"),      // empty index
		[]byte("o:not-a-uuid:0001"),            // bad signal ID
	} {
		_, _, ok := ParseOutlierFacet(key)
		assert.False(t, ok, "key %q must not parse", key)
	}
}

func TestSignalLocSet_RoundTrip(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	in := []FacetLoc{
		{StoryID: a},
		{IsOutlier: true},
		{StoryID: b},
		{}, // no location recorded
	}

	out, ok := ParseSignalLocSet(EncodeSignalLocSet(in))
	require.True(t, ok)
	assert.Equal(t, in, out)
}

func TestSignalLocSet_Empty(t *testing.T) {
	out, ok := ParseSignalLocSet(EncodeSignalLocSet(nil))
	require.True(t, ok)
	assert.Empty(t, out)
}

func TestSignalLocSet_RejectsMalformed(t *testing.T) {
	validBytes := EncodeSignalLocSet([]FacetLoc{{StoryID: uuid.New()}, {IsOutlier: true}})
	truncated := validBytes[:len(validBytes)-2]

	for _, val := range [][]byte{
		[]byte(`not json`),
		[]byte(`{"a":1}`),
		[]byte(`["x:whatever"]`),
		[]byte(`["s:11111111-2222-3333-4444-555555555555","o"]`), // legacy JSON array
		{0xff, 0xff}, // invalid CBOR
		truncated,    // truncated CBOR
	} {
		_, ok := ParseSignalLocSet(val)
		assert.False(t, ok, "value %q must not parse", val)
	}
}
