package story

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sampleKeyAsIntStruct struct {
	ID   uuid.UUID `cbor:"0,keyasint"`
	Name string    `cbor:"1,keyasint"`
}

type extraFieldStruct struct {
	ID    uuid.UUID `cbor:"0,keyasint"`
	Name  string    `cbor:"1,keyasint"`
	Extra int       `cbor:"2,keyasint"`
}

func TestCBOR_UUIDEncoding(t *testing.T) {
	id := uuid.MustParse("12345678-1234-5678-1234-567812345678")
	b, err := cborEncMode.Marshal(id)
	require.NoError(t, err)

	// uuid.UUID is [16]byte and implements encoding.BinaryMarshaler.
	// In CBOR, a 16-byte byte string is 0x50 (major type 2, length 16) followed by 16 bytes = 17 bytes total.
	t.Logf("Encoded uuid.UUID byte length: %d (hex: %x)", len(b), b)
	assert.Equal(t, 17, len(b), "expected uuid.UUID to encode as 17-byte byte string")
	assert.Equal(t, byte(0x50), b[0], "expected byte string major type 2 with length 16")

	var got uuid.UUID
	err = cborDecMode.Unmarshal(b, &got)
	require.NoError(t, err)
	assert.Equal(t, id, got)
}

func TestCBOR_TimeNanosecondRoundTrip(t *testing.T) {
	now := time.Now().UTC()
	// Ensure sub-second precision is non-zero
	if now.Nanosecond() == 0 {
		now = now.Add(123456789 * time.Nanosecond)
	}

	b, err := cborEncMode.Marshal(now)
	require.NoError(t, err)

	var gotDec time.Time
	err = cborDecMode.Unmarshal(b, &gotDec)
	require.NoError(t, err)
	assert.True(t, now.Equal(gotDec), "cborDecMode must preserve exact nanosecond timestamp")
	assert.Equal(t, now.Nanosecond(), gotDec.Nanosecond(), "nanoseconds must match exactly")

	var gotStrict time.Time
	err = cborStrictDecMode.Unmarshal(b, &gotStrict)
	require.NoError(t, err)
	assert.True(t, now.Equal(gotStrict), "cborStrictDecMode must preserve exact nanosecond timestamp")
	assert.Equal(t, now.Nanosecond(), gotStrict.Nanosecond(), "nanoseconds must match exactly")
}

func TestCBOR_StrictUnknownField(t *testing.T) {
	full := extraFieldStruct{
		ID:    uuid.New(),
		Name:  "test",
		Extra: 42,
	}

	b, err := cborEncMode.Marshal(full)
	require.NoError(t, err)

	// Lenient dec mode should ignore extra field
	var lenient sampleKeyAsIntStruct
	err = cborDecMode.Unmarshal(b, &lenient)
	require.NoError(t, err)
	assert.Equal(t, full.ID, lenient.ID)
	assert.Equal(t, full.Name, lenient.Name)

	// Strict dec mode should return error on unknown field
	var strict sampleKeyAsIntStruct
	err = cborStrictDecMode.Unmarshal(b, &strict)
	require.Error(t, err, "expected strict mode to reject unknown field")
}
