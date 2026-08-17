// Package story provides a streaming story tracker: a hybrid clustering
// system that ingests a continuous stream of signals and groups them into
// evolving stories.
//
// Ingestion is two-phase:
//
//   - Real-time Draft phase: each signal is immediately assigned to the
//     nearest story centroid, or held as an outlier when no story covers it.
//   - Periodic maintenance phase: a background batch run maintains the
//     existing stories rather than re-deriving them, promoting groups of
//     outliers, admitting outliers into stories that cover them, splitting
//     stories that have diverged, and merging those that have converged.
//     Story identity therefore survives every run.
//
// Distances are measured in centred space: the corpus mean is subtracted from
// every embedding before any comparison, which is what keeps anisotropic text
// embeddings from collapsing into a single story. Every threshold in Config is
// a centred-space distance. See DESIGN.md.
//
// Signal IDs are UUID v5, derived from a caller domain key. Prefer
// Tracker.SignalID, which honours a configured Config.Namespace:
//
//	id := tracker.SignalID(domainKey)
//
// Story IDs are UUID v5 as well, derived from the signals a story was founded
// on rather than drawn at random, so replaying a signal stream against a fresh
// store reproduces the same story IDs.
//
// Create a Tracker by supplying a Config with at least a Store and Codec:
//
//	t, err := story.NewTracker(story.Config[MyData]{
//	    Store: myStore,
//	    Codec: myCodec,
//	})
package story
