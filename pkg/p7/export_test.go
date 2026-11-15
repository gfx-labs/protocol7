package p7

import "time"

// LimitCapacity is capacityOf, for the tests in package p7_test.
func LimitCapacity(c LimitConfig, name string) (int, bool) { return c.capacityOf(name) }

// NextWaiter is the index of the waiter a limit serves first, given each
// waiter's priority and how long it has waited, in arrival order.
func NextWaiter(aging time.Duration, now time.Time, waiters []LimitEntry) int {
	state := limitState{Aging: aging, Waiters: waiters}
	return state.next(now)
}
