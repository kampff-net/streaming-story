package story

import "encoding/json"

// JSONCodec encodes signals with encoding/json. It is the default choice when
// the payload type T is JSON-serialisable; callers with large embeddings or
// strict latency budgets should supply a binary Codec instead.
type JSONCodec[T any] struct{}

// Compile-time interface check.
var _ Codec[struct{}] = JSONCodec[struct{}]{}

// Encode implements Codec.
func (JSONCodec[T]) Encode(sig Signal[T]) ([]byte, error) { return json.Marshal(sig) }

// Decode implements Codec.
func (JSONCodec[T]) Decode(b []byte) (Signal[T], error) {
	var sig Signal[T]
	if err := json.Unmarshal(b, &sig); err != nil {
		return Signal[T]{}, err
	}
	return sig, nil
}
