// Package domain holds the plain types that cross a process boundary in
// protocol7: activity inputs and results, session events and stream frames.
// Consumers use them through the aliases in pkg/p7.
//
// The plan (docs/plan.md, section 6) says these are protobuf messages. They
// are Go structs with JSON tags until the protobuf schemas are written.
package domain
