package story

import (
	"encoding/json"
)

func newMemStore() *MemStore {
	return NewMemStore()
}

// jsonCodec[T] is a test Codec that uses encoding/json.
type jsonCodec[T any] struct{}

func (jsonCodec[T]) Encode(sig Signal[T]) ([]byte, error) { return json.Marshal(sig) }
func (jsonCodec[T]) Decode(b []byte) (Signal[T], error) {
	var sig Signal[T]
	return sig, json.Unmarshal(b, &sig)
}
