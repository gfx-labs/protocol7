package issue

import "time"

// Labelled and the scan's limits are exported for the tests.
var Labelled = (*Activities).labelled

func ScanWorstCase() time.Duration { return scanSettle + worstCase(scanOptions) }

// ScanSettle is the wait before a woken pass reads GitHub.
const ScanSettle = scanSettle
