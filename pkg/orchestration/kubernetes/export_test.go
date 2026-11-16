package kubernetes

import "time"

// SetPoolTimings shortens the timings of a pool for a test, and returns what
// restores them.
func SetPoolTimings(missing, orphan, check time.Duration) func() {
	oldMissing, oldOrphan, oldCheck := poolMissingAfter, poolOrphanAge, poolCheckEvery
	poolMissingAfter, poolOrphanAge, poolCheckEvery = missing, orphan, check
	return func() { poolMissingAfter, poolOrphanAge, poolCheckEvery = oldMissing, oldOrphan, oldCheck }
}
