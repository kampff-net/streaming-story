package story

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMemStore() *MemStore {
	return NewMemStore()
}

// errTestStop stops a scan early in tests that assert on that path.
var errTestStop = errors.New("stop")

func TestMemStore_PutGet(t *testing.T) {
	ms := newMemStore()
	key := []byte("k")

	require.NoError(t, ms.Update(func(tx Tx) error { return tx.Put(key, []byte("v")) }))
	v, err := ms.Get(key)
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)

	// Get returns nil for missing keys, not an empty slice.
	v, err = ms.Get([]byte("missing"))
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestMemStore_PutRejectsEmptyValue(t *testing.T) {
	ms := newMemStore()
	err := ms.Update(func(tx Tx) error { return tx.Put([]byte("k"), nil) })
	require.Error(t, err)
}

func TestMemStore_GetReturnsCopy(t *testing.T) {
	ms := newMemStore()
	key := []byte("k")
	require.NoError(t, ms.Update(func(tx Tx) error { return tx.Put(key, []byte("original")) }))

	first, err := ms.Get(key)
	require.NoError(t, err)
	first[0] = 'X'

	v, err := ms.Get(key)
	require.NoError(t, err)
	assert.Equal(t, "original", string(v), "mutation of a returned slice must not corrupt the store")
}

func TestMemStore_Delete(t *testing.T) {
	ms := newMemStore()
	require.NoError(t, ms.Update(func(tx Tx) error { return tx.Put([]byte("a"), []byte("1")) }))
	require.NoError(t, ms.Update(func(tx Tx) error { return tx.Delete([]byte("a")) }))
	// Deleting a missing key is not an error.
	require.NoError(t, ms.Update(func(tx Tx) error { return tx.Delete([]byte("a")) }))

	v, err := ms.Get([]byte("a"))
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestMemStore_DeletePrefix(t *testing.T) {
	ms := newMemStore()
	require.NoError(t, ms.Update(func(tx Tx) error {
		for _, k := range [][]byte{[]byte("s:a"), []byte("s:ab"), []byte("s:b"), []byte("o:x")} {
			if err := tx.Put(k, []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, ms.Update(func(tx Tx) error { return tx.DeletePrefix([]byte("s:")) }))

	for _, k := range [][]byte{[]byte("s:a"), []byte("s:ab"), []byte("s:b")} {
		v, err := ms.Get(k)
		require.NoError(t, err)
		assert.Nil(t, v, "key %q must be deleted", k)
	}
	v, err := ms.Get([]byte("o:x"))
	require.NoError(t, err)
	assert.NotNil(t, v, "unrelated key must survive")
}

func TestMemStore_ScanPrefix(t *testing.T) {
	ms := newMemStore()
	require.NoError(t, ms.Update(func(tx Tx) error {
		for _, k := range []string{"s:b", "s:a", "o:x", "s:c"} {
			if err := tx.Put([]byte(k), []byte(k)); err != nil {
				return err
			}
		}
		return nil
	}))

	var got []string
	require.NoError(t, ms.ScanPrefix([]byte("s:"), func(key, val []byte) error {
		got = append(got, string(key))
		return nil
	}))
	assert.Equal(t, []string{"s:a", "s:b", "s:c"}, got)

	// fn error stops iteration.
	var n int
	err := ms.ScanPrefix([]byte("s:"), func(key, val []byte) error {
		n++
		return errTestStop
	})
	require.Error(t, err)
	assert.Equal(t, 1, n)
}

func TestMemStore_ScanRange(t *testing.T) {
	ms := newMemStore()
	require.NoError(t, ms.Update(func(tx Tx) error {
		for _, k := range []string{"a", "b", "c", "d"} {
			if err := tx.Put([]byte(k), []byte(k)); err != nil {
				return err
			}
		}
		return nil
	}))

	var got []string
	require.NoError(t, ms.ScanRange([]byte("b"), []byte("d"), func(key, val []byte) error {
		got = append(got, string(key))
		return nil
	}))
	assert.Equal(t, []string{"b", "c"}, got, "ScanRange is [from, to) exclusive of to")
}
