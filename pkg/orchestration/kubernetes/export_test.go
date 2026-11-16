package kubernetes

import "time"

// SetPoolTimings shortens the timings of a pool for a test, and returns what
// restores them.
func SetPoolTimings(missing, orphan, check time.Duration) func() {
	oldMissing, oldOrphan, oldCheck := poolMissingAfter, poolOrphanAge, poolCheckEvery
	poolMissingAfter, poolOrphanAge, poolCheckEvery = missing, orphan, check
	return func() { poolMissingAfter, poolOrphanAge, poolCheckEvery = oldMissing, oldOrphan, oldCheck }
}

// SetPoolWaiting shortens how often a waiting session asks its pool, and how
// long the pool keeps one that stopped asking, for a test. The ask may be
// changed while sessions wait; the time to keep may not.
func SetPoolWaiting(ask, ttl time.Duration) func() {
	oldAsk, oldTTL := time.Duration(poolAskEvery.Swap(int64(ask))), poolWaiterTTL
	poolWaiterTTL = ttl
	return func() { poolAskEvery.Store(int64(oldAsk)); poolWaiterTTL = oldTTL }
}

// SetPoolAsk is SetPoolWaiting's ask alone.
func SetPoolAsk(ask time.Duration) { poolAskEvery.Store(int64(ask)) }
