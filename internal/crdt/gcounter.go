// Package crdt implements conflict-free replicated data types used to
// synchronize distributed state without central coordination.
package crdt

// GCounter is a grow-only counter CRDT: each node increments only its own
// component, and any two replicas can be merged by taking the per-node
// maximum, guaranteeing eventual consistency regardless of merge order.
type GCounter struct {
	counts map[string]int
}

// NewGCounter returns an empty counter.
func NewGCounter() *GCounter {
	return &GCounter{counts: make(map[string]int)}
}

// Increment adds delta to nodeID's own component. A node must never
// increment any component but its own.
func (c *GCounter) Increment(nodeID string, delta int) {
	if c.counts == nil {
		c.counts = make(map[string]int)
	}
	c.counts[nodeID] += delta
}

// Merge folds other into c by taking, for every node, the maximum of the two
// components. Merge is commutative, associative and idempotent, so it is
// safe to call in any order or more than once with the same data.
func (c *GCounter) Merge(other *GCounter) {
	if other == nil {
		return
	}
	if c.counts == nil {
		c.counts = make(map[string]int)
	}
	for nodeID, otherCount := range other.counts {
		if otherCount > c.counts[nodeID] {
			c.counts[nodeID] = otherCount
		}
	}
}

// Value returns the counter's total: the sum of every node's component.
func (c *GCounter) Value() int {
	total := 0
	for _, count := range c.counts {
		total += count
	}
	return total
}

// Snapshot returns a defensive copy of the counter's internal state, e.g.
// for shipping over the wire in a MergeRequest.
func (c *GCounter) Snapshot() map[string]int {
	snapshot := make(map[string]int, len(c.counts))
	for nodeID, count := range c.counts {
		snapshot[nodeID] = count
	}
	return snapshot
}

// FromSnapshot rebuilds a GCounter from a previously captured Snapshot.
func FromSnapshot(snapshot map[string]int) *GCounter {
	c := NewGCounter()
	for nodeID, count := range snapshot {
		c.counts[nodeID] = count
	}
	return c
}
