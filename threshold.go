package story

import "go.kvsh.ch/streaming-story/internal/geom"

// Threshold policy: how wide a story's catchment is at a given moment.
// Shared by the Draft phase and by outlier admission, so both admit on the
// same terms.

// calcThreshold calculates the dynamic distance threshold T_assign(story).
//
// T_assign(story) = mean_distance(story) + AssignmentK × σ(story).
//
// Dormant stories use the statistics frozen at the Dormant transition.
// Active stories use live per-story statistics once the story has reached
// ColdStartMinSignals window signals; below that the story is in cold-start
// and falls back to AssignmentK × σ_global. σ is floored at
// SigmaFloor × σ_global to prevent the threshold collapsing on near-identical
// first signals.
func (t *Tracker[T]) calcThreshold(story StoryMeta) float64 {
	t.calibMu.RLock()
	sigmaGlobal := t.sigmaGlobal
	t.calibMu.RUnlock()

	if sigmaGlobal == 0 {
		// No batch has completed yet, so σ_global has never been measured.
		// InitialSigmaGlobal stands in until the first run seeds it.
		sigmaGlobal = t.cfg.InitialSigmaGlobal
	}
	floor := t.cfg.SigmaFloor * sigmaGlobal

	if story.State == StoryStateDormant {
		sigma := story.FrozenSigma
		if sigma == 0 {
			sigma = sigmaGlobal
		}
		if sigma < floor {
			sigma = floor
		}
		return t.clampAssign(story.FrozenMeanDistance + t.cfg.AssignmentK*sigma)
	}

	if story.SignalCount >= t.cfg.ColdStartMinSignals {
		sigma := story.Sigma
		if sigma < floor {
			sigma = floor
		}
		return t.clampAssign(story.MeanDistance + t.cfg.AssignmentK*sigma)
	}

	return t.clampAssign(t.cfg.AssignmentK * sigmaGlobal)
}

// clampAssign bounds an adaptive threshold by AssignThreshold. Without the
// clamp a story that has drifted wide keeps widening its own catchment, which
// is how a single story ends up absorbing unrelated coverage.
func (t *Tracker[T]) clampAssign(d float64) float64 {
	// Zero means unset, which happens only for a Config that skipped
	// validate(); clamping to zero there would reject every assignment.
	if t.cfg.AssignThreshold <= 0 {
		return d
	}
	if d > t.cfg.AssignThreshold {
		return t.cfg.AssignThreshold
	}
	return d
}

// admissionThreshold mirrors calcThreshold for a story whose live geometry the
// maintenance pass already holds, so a batch admission and a Draft assignment
// apply the same radius to the same story.
func (t *Tracker[T]) admissionThreshold(st geom.Stats, n int) float64 {
	t.calibMu.RLock()
	sigmaGlobal := t.sigmaGlobal
	t.calibMu.RUnlock()
	if sigmaGlobal == 0 {
		sigmaGlobal = t.cfg.InitialSigmaGlobal
	}
	if n < t.cfg.ColdStartMinSignals {
		return t.clampAssign(t.cfg.AssignmentK * sigmaGlobal)
	}
	sigma := max(st.Sigma, t.cfg.SigmaFloor*sigmaGlobal)
	return t.clampAssign(st.Mean + t.cfg.AssignmentK*sigma)
}
