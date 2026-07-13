package crdt

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIncrementOnlyAffectsOwnNode(t *testing.T) {
	c := NewGCounter()
	c.Increment("node-a", 3)
	c.Increment("node-a", 2)
	c.Increment("node-b", 10)

	assert.Equal(t, map[string]int{"node-a": 5, "node-b": 10}, c.Snapshot())
	assert.Equal(t, 15, c.Value())
}

func TestMergeTakesPerNodeMax(t *testing.T) {
	a := NewGCounter()
	a.Increment("node-a", 5)
	a.Increment("node-b", 1)

	b := NewGCounter()
	b.Increment("node-a", 2) // stale relative to a
	b.Increment("node-b", 7) // ahead of a
	b.Increment("node-c", 4) // unseen by a

	a.Merge(b)

	assert.Equal(t, map[string]int{"node-a": 5, "node-b": 7, "node-c": 4}, a.Snapshot())
	assert.Equal(t, 16, a.Value())
}

func TestMergeIsIdempotent(t *testing.T) {
	a := NewGCounter()
	a.Increment("node-a", 5)
	b := NewGCounter()
	b.Increment("node-b", 3)

	a.Merge(b)
	first := a.Snapshot()
	a.Merge(b)
	a.Merge(b)

	assert.Equal(t, first, a.Snapshot())
}

func TestMergeIsCommutativeAndAssociativeUnderRandomOrder(t *testing.T) {
	replicas := make([]*GCounter, 4)
	for i := range replicas {
		replicas[i] = NewGCounter()
		replicas[i].Increment("node-a", i+1)
		replicas[i].Increment("node-b", (i+1)*2)
	}

	merged := func(order []int) map[string]int {
		result := NewGCounter()
		for _, idx := range order {
			result.Merge(replicas[idx])
		}
		return result.Snapshot()
	}

	baseline := merged([]int{0, 1, 2, 3})
	for trial := 0; trial < 10; trial++ {
		order := rand.Perm(len(replicas))
		assert.Equal(t, baseline, merged(order))
	}
}

func TestFromSnapshotRoundTrip(t *testing.T) {
	original := NewGCounter()
	original.Increment("node-a", 7)
	original.Increment("node-b", 3)

	restored := FromSnapshot(original.Snapshot())
	assert.Equal(t, original.Snapshot(), restored.Snapshot())
	assert.Equal(t, original.Value(), restored.Value())
}
