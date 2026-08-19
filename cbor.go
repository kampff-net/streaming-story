package story

import (
	"github.com/fxamacker/cbor/v2"
)

var (
	cborEncMode = mustEncMode()

	// cborDecMode decodes caller-facing values: Signal[T], whose Data field is
	// the caller's own type. Lenient, because a caller that drops a field from
	// T must still be able to read records written before the drop.
	cborDecMode = mustDecMode(cbor.DecOptions{})

	// cborStrictDecMode decodes this library's own records, whose schema only
	// this repo changes. A key the current schema does not know is a downgrade
	// or a corrupt value, and failing loudly beats decoding a partial record.
	cborStrictDecMode = mustDecMode(cbor.DecOptions{
		ExtraReturnErrors: cbor.ExtraDecErrorUnknownField,
	})
)

func mustEncMode() cbor.EncMode {
	opts := cbor.CanonicalEncOptions()
	opts.Time = cbor.TimeRFC3339Nano
	opts.TimeTag = cbor.EncTagNone
	m, err := opts.EncMode()
	if err != nil {
		panic("story: cbor enc mode: " + err.Error())
	}
	return m
}

func mustDecMode(opts cbor.DecOptions) cbor.DecMode {
	m, err := opts.DecMode()
	if err != nil {
		panic("story: cbor dec mode: " + err.Error())
	}
	return m
}
