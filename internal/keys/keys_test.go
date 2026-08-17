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
}

func TestKeyStoryPrefix(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:"),
		StoryPrefix(keysTestStoryID),
	)
}

func TestKeySignal(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:s:11111111-2222-3333-4444-555555555555"),
		Signal(keysTestStoryID, keysTestSignalID),
	)
}

func TestKeySignalPrefix(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:s:"),
		SignalPrefix(keysTestStoryID),
	)
}

func TestKeyOutlier(t *testing.T) {
	assert.Equal(t,
		[]byte("o:11111111-2222-3333-4444-555555555555"),
		Outlier(keysTestSignalID),
	)
}

func TestKeySignalLoc(t *testing.T) {
	assert.Equal(t,
		[]byte("l:11111111-2222-3333-4444-555555555555"),
		SignalLoc(keysTestSignalID),
	)
}

func TestParseSignalIDFromLocKey(t *testing.T) {
	id, ok := ParseSignalIDFromLoc(Signal(keysTestStoryID, keysTestSignalID))
	require.True(t, ok)
	assert.Equal(t, keysTestSignalID, id)

	id, ok = ParseSignalIDFromLoc(Outlier(keysTestSignalID))
	require.True(t, ok)
	assert.Equal(t, keysTestSignalID, id)

	_, ok = ParseSignalIDFromLoc([]byte("s:story:m"))
	assert.False(t, ok)
	_, ok = ParseSignalIDFromLoc([]byte("l:whatever"))
	assert.False(t, ok)
	_, ok = ParseSignalIDFromLoc(nil)
	assert.False(t, ok)
}

func TestParseStoryIDFromSignalKey(t *testing.T) {
	id, ok := ParseStoryIDFromSignal(Signal(keysTestStoryID, keysTestSignalID))
	require.True(t, ok)
	assert.Equal(t, keysTestStoryID, id)

	_, ok = ParseStoryIDFromSignal(Outlier(keysTestSignalID))
	assert.False(t, ok)
	_, ok = ParseStoryIDFromSignal([]byte("s:story:m"))
	assert.False(t, ok)
}

func TestIsOutlierKey(t *testing.T) {
	assert.True(t, IsOutlier(Outlier(keysTestSignalID)))
	assert.False(t, IsOutlier(Signal(keysTestStoryID, keysTestSignalID)))
	assert.False(t, IsOutlier(nil))
}

func TestParseSignalLoc(t *testing.T) {
	storyID := uuid.New()

	id, isOutlier, ok := ParseSignalLoc([]byte("s:" + storyID.String()))
	require.True(t, ok)
	assert.Equal(t, storyID, id)
	assert.False(t, isOutlier)

	_, isOutlier, ok = ParseSignalLoc([]byte("o"))
	require.True(t, ok)
	assert.True(t, isOutlier)

	_, _, ok = ParseSignalLoc([]byte("s:not-a-uuid"))
	assert.False(t, ok)
	_, _, ok = ParseSignalLoc([]byte("x"))
	assert.False(t, ok)
	_, _, ok = ParseSignalLoc(nil)
	assert.False(t, ok)
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
