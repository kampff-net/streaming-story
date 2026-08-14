package story

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMemStore() *MemStore {
	return NewMemStore()
}

func TestMemStore_PutGet(t *testing.T) {
	ms := newMemStore()
	key := []byte("k")

	require.NoError(t, ms.Update(func(tx Tx) error { return tx.Put(key, []byte("v")) }))
	require.NoError(t, ms.View(func(tx Tx) error {
		v, err := tx.Get(key)
		require.NoError(t, err)
		assert.Equal(t, []byte("v"), v)
		return nil
	}))

	// Get returns nil for missing keys, not an empty slice.
	require.NoError(t, ms.View(func(tx Tx) error {
		v, err := tx.Get([]byte("missing"))
		require.NoError(t, err)
		assert.Nil(t, v)
		return nil
	}))
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

	var first []byte
	require.NoError(t, ms.View(func(tx Tx) error {
		v, err := tx.Get(key)
		require.NoError(t, err)
		first = v
		first[0] = 'X'
		return nil
	}))

	require.NoError(t, ms.View(func(tx Tx) error {
		v, err := tx.Get(key)
		require.NoError(t, err)
		assert.Equal(t, "original", string(v), "mutation of a returned slice must not corrupt the store")
		return nil
	}))
}

func TestMemStore_Delete(t *testing.T) {
	ms := newMemStore()
	require.NoError(t, ms.Update(func(tx Tx) error { return tx.Put([]byte("a"), []byte("1")) }))
	require.NoError(t, ms.Update(func(tx Tx) error { return tx.Delete([]byte("a")) }))
	// Deleting a missing key is not an error.
	require.NoError(t, ms.Update(func(tx Tx) error { return tx.Delete([]byte("a")) }))

	require.NoError(t, ms.View(func(tx Tx) error {
		v, err := tx.Get([]byte("a"))
		require.NoError(t, err)
		assert.Nil(t, v)
		return nil
	}))
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

	require.NoError(t, ms.View(func(tx Tx) error {
		for _, k := range [][]byte{[]byte("s:a"), []byte("s:ab"), []byte("s:b")} {
			v, err := tx.Get(k)
			require.NoError(t, err)
			assert.Nil(t, v, "key %q must be deleted", k)
		}
		v, err := tx.Get([]byte("o:x"))
		require.NoError(t, err)
		assert.NotNil(t, v, "unrelated key must survive")
		return nil
	}))
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
	require.NoError(t, ms.View(func(tx Tx) error {
		return tx.ScanPrefix([]byte("s:"), func(key, val []byte) error {
			got = append(got, string(key))
			return nil
		})
	}))
	assert.Equal(t, []string{"s:a", "s:b", "s:c"}, got)

	// fn error stops iteration.
	var n int
	err := ms.View(func(tx Tx) error {
		return tx.ScanPrefix([]byte("s:"), func(key, val []byte) error {
			n++
			return errStopIteration
		})
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
	require.NoError(t, ms.View(func(tx Tx) error {
		return tx.ScanRange([]byte("b"), []byte("d"), func(key, val []byte) error {
			got = append(got, string(key))
			return nil
		})
	}))
	assert.Equal(t, []string{"b", "c"}, got, "ScanRange is [from, to) exclusive of to")
}
