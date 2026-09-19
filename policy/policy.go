// Package policy holds error policies for chunkflow pipelines: transforms that look at
// the errors flowing through a stream and decide which of them the pipeline tolerates.
// A policy is plugged in with Stream.Through:
//
//	users.Through(policy.CircuitBreaker[User](5))
//
// The element type cannot be inferred from a policy's arguments, so it is spelled out.
//
// Everything here is written on the public extension seam of the root package and
// nothing else: Stream.Transform hands a policy the raw (value, error) sequence,
// including fatal errors, and Suppress is the only way to mark an error as tolerated.
// A policy in another module has exactly the same tools.
package policy
