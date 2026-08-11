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
