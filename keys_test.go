package story

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
	assert.Equal(t, []byte("c:state"), keyCalibState())
}

func TestKeyStoryMeta(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:m"),
		keyStoryMeta(keysTestStoryID),
	)
}

func TestKeyStoryPrefix(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:"),
		keyStoryPrefix(keysTestStoryID),
	)
}

func TestKeySignal(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:s:11111111-2222-3333-4444-555555555555"),
		keySignal(keysTestStoryID, keysTestSignalID),
	)
}

func TestKeySignalPrefix(t *testing.T) {
	assert.Equal(t,
		[]byte("s:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:s:"),
		keySignalPrefix(keysTestStoryID),
	)
}

func TestKeyOutlier(t *testing.T) {
	assert.Equal(t,
		[]byte("o:11111111-2222-3333-4444-555555555555"),
		keyOutlier(keysTestSignalID),
	)
}

func TestKeySignalLoc(t *testing.T) {
	assert.Equal(t,
		[]byte("l:11111111-2222-3333-4444-555555555555"),
		keySignalLoc(keysTestSignalID),
	)
}

func TestParseSignalIDFromLocKey(t *testing.T) {
	id, ok := parseSignalIDFromLocKey(keySignal(keysTestStoryID, keysTestSignalID))
	require.True(t, ok)
	assert.Equal(t, keysTestSignalID, id)

	id, ok = parseSignalIDFromLocKey(keyOutlier(keysTestSignalID))
	require.True(t, ok)
	assert.Equal(t, keysTestSignalID, id)

	_, ok = parseSignalIDFromLocKey([]byte("s:story:m"))
	assert.False(t, ok)
	_, ok = parseSignalIDFromLocKey([]byte("l:whatever"))
	assert.False(t, ok)
	_, ok = parseSignalIDFromLocKey(nil)
	assert.False(t, ok)
}

func TestParseStoryIDFromSignalKey(t *testing.T) {
	id, ok := parseStoryIDFromSignalKey(keySignal(keysTestStoryID, keysTestSignalID))
	require.True(t, ok)
	assert.Equal(t, keysTestStoryID, id)

	_, ok = parseStoryIDFromSignalKey(keyOutlier(keysTestSignalID))
	assert.False(t, ok)
	_, ok = parseStoryIDFromSignalKey([]byte("s:story:m"))
	assert.False(t, ok)
}

func TestIsOutlierKey(t *testing.T) {
	assert.True(t, isOutlierKey(keyOutlier(keysTestSignalID)))
	assert.False(t, isOutlierKey(keySignal(keysTestStoryID, keysTestSignalID)))
	assert.False(t, isOutlierKey(nil))
}

func TestParseSignalLoc(t *testing.T) {
	storyID := uuid.New()

	id, isOutlier, ok := parseSignalLoc([]byte("s:" + storyID.String()))
	require.True(t, ok)
	assert.Equal(t, storyID, id)
	assert.False(t, isOutlier)

	_, isOutlier, ok = parseSignalLoc([]byte("o"))
	require.True(t, ok)
	assert.True(t, isOutlier)

	_, _, ok = parseSignalLoc([]byte("s:not-a-uuid"))
	assert.False(t, ok)
	_, _, ok = parseSignalLoc([]byte("x"))
	assert.False(t, ok)
	_, _, ok = parseSignalLoc(nil)
	assert.False(t, ok)
}

func TestParseTimeIndexKey(t *testing.T) {
	id := uuid.New()
	key := keyTimeIndex(1_700_000_000, id)
	got, ok := parseTimeIndexKey(key)
	require.True(t, ok)
	assert.Equal(t, id, got)

	_, ok = parseTimeIndexKey([]byte("s:story:m"))
	assert.False(t, ok)
	_, ok = parseTimeIndexKey([]byte("t:abc"))
	assert.False(t, ok)
	_, ok = parseTimeIndexKey([]byte("t:"))
	assert.False(t, ok)
}

func TestKeyTimeIndex(t *testing.T) {
	t.Run("exact_format", func(t *testing.T) {
		assert.Equal(t,
			[]byte("t:1234567890:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			keyTimeIndex(1234567890, keysTestStoryID),
		)
	})

	t.Run("zero_pads_to_10_digits", func(t *testing.T) {
		assert.Equal(t,
			[]byte("t:0000000001:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			keyTimeIndex(1, keysTestStoryID),
		)
	})

	t.Run("lexicographic_order_matches_chronological_order", func(t *testing.T) {
		earlier := keyTimeIndex(1_000_000_000, keysTestStoryID)
		later := keyTimeIndex(2_000_000_000, keysTestStoryID)
		assert.Negative(t, bytes.Compare(earlier, later),
			"earlier timestamp must produce a key that sorts before later timestamp")
	})
}

func TestKeyTimeIndexFrom(t *testing.T) {
	assert.Equal(t, []byte("t:1234567890:"), keyTimeIndexFrom(1234567890))
}
