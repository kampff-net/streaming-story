package dist

import (
	"gonum.org/v1/gonum/blas/blas32"
	"gonum.org/v1/gonum/blas/gonum"
)

func init() {
	blas32.Use(gonum.Implementation{})
}

// CosineSimilarity returns the cosine similarity between two vectors.
// It returns 0 if either vector has a zero norm.
// Vectors must have the same length.
func CosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	dot := Dot(a, b)
	normA := Norm(a)
	normB := Norm(b)

	if normA <= 0 || normB <= 0 {
		return 0
	}
	sim := dot / (normA * normB)
	if sim > 1 {
		return 1
	} else if sim < -1 {
		return -1
	}
	return sim
}

// CosineDistance returns the cosine distance (1 - similarity) between vectors.
func CosineDistance(a, b []float32) float64 {
	sim := CosineSimilarity(a, b)
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return 1.0 - float64(sim)
}

// Dot returns the dot product of two vectors using optimized BLAS.
func Dot(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	return blas32.Dot(
		blas32.Vector{N: len(a), Data: a, Inc: 1},
		blas32.Vector{N: len(b), Data: b, Inc: 1},
	)
}

// Norm returns the Euclidean norm of a vector using optimized BLAS.
func Norm(a []float32) float32 {
	if len(a) == 0 {
		return 0
	}
	return blas32.Nrm2(blas32.Vector{N: len(a), Data: a, Inc: 1})
}

// DotUnit returns the dot product of two unit vectors.
func DotUnit(a, b []float32) float32 {
	return Dot(a, b)
}

// CosineDistanceUnit returns the cosine distance between two vectors the
// caller guarantees are unit length. It is 1 - Dot with no norm computed;
// passing a non-unit vector returns a meaningless number rather than an error,
// which is why the name says Unit.
//
// Nothing in this library calls it. Every distance the tracker measures is
// between projected vectors — the corpus mean subtracted — and a projected
// vector is not unit. Making them unit would mean renormalizing the residual,
// which changes what geom.Centroid averages and moves cluster boundaries; see
// spec 008 §2.5. Stored embeddings are unit before projection, so a caller
// measuring those may use this; measure what you are passing first.
func CosineDistanceUnit(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 1.0
	}
	sim := DotUnit(a, b)
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return 1.0 - float64(sim)
}
