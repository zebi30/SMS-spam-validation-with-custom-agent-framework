package registry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/registry/registrypb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

// stateCollector is a minimal actor used only to receive RegistryState replies in tests.
type stateCollector struct {
	id     string
	states chan RegistryState
}

func newStateCollector(id string) *stateCollector {
	return &stateCollector{id: id, states: make(chan RegistryState, 8)}
}

func (c *stateCollector) ID() string { return c.id }

func (c *stateCollector) Receive(ctx *actor.Context, msg actor.Message) {
	if state, ok := msg.(RegistryState); ok {
		c.states <- state
	}
}

func awaitState(t *testing.T, ch chan RegistryState) RegistryState {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RegistryState")
		return RegistryState{}
	}
}

func TestNodeJoinLeaveTracksActiveSet(t *testing.T) {
	system := actor.NewActorSystem()
	defer system.Shutdown()

	registryPID, err := system.Spawn(NewRegistryActor("registry"))
	require.NoError(t, err)

	collector := newStateCollector("collector")
	collectorPID, err := system.Spawn(collector)
	require.NoError(t, err)

	require.NoError(t, registryPID.Tell(&registrypb.NodeJoin{NodeId: "node-a"}))
	require.NoError(t, registryPID.Tell(&registrypb.NodeJoin{NodeId: "node-b"}))
	require.NoError(t, registryPID.Tell(QueryState{ReplyTo: collectorPID}))

	state := awaitState(t, collector.states)
	assert.Equal(t, []string{"node-a", "node-b"}, state.ActiveNodes)

	require.NoError(t, registryPID.Tell(&registrypb.NodeLeave{NodeId: "node-a"}))
	require.NoError(t, registryPID.Tell(QueryState{ReplyTo: collectorPID}))

	state = awaitState(t, collector.states)
	assert.Equal(t, []string{"node-b"}, state.ActiveNodes)
}

func TestRoundCompleteIncrementsOwnComponentOnly(t *testing.T) {
	system := actor.NewActorSystem()
	defer system.Shutdown()

	registryPID, err := system.Spawn(NewRegistryActor("registry"))
	require.NoError(t, err)

	collector := newStateCollector("collector")
	collectorPID, err := system.Spawn(collector)
	require.NoError(t, err)

	require.NoError(t, registryPID.Tell(&registrypb.RoundComplete{NodeId: "node-a"}))
	require.NoError(t, registryPID.Tell(&registrypb.RoundComplete{NodeId: "node-a"}))
	require.NoError(t, registryPID.Tell(&registrypb.RoundComplete{NodeId: "node-b"}))
	require.NoError(t, registryPID.Tell(QueryState{ReplyTo: collectorPID}))

	state := awaitState(t, collector.states)
	assert.Equal(t, 3, state.CompletedRounds)
	assert.Equal(t, map[string]int{"node-a": 2, "node-b": 1}, state.Counter)
}

// TestTwoRegistriesConvergeViaMerge simulates the CLUSTER LAYER's CRDT merge:
// two independent RegistryActor instances (as if on two different nodes)
// each record local rounds, gossip MergeRequests to each other, and must end
// up agreeing on the total completed round count regardless of gossip order.
func TestTwoRegistriesConvergeViaMerge(t *testing.T) {
	system := actor.NewActorSystem()
	defer system.Shutdown()

	registryA, err := system.Spawn(NewRegistryActor("registry-a"))
	require.NoError(t, err)
	registryB, err := system.Spawn(NewRegistryActor("registry-b"))
	require.NoError(t, err)

	collectorA := newStateCollector("collector-a")
	collectorAPID, err := system.Spawn(collectorA)
	require.NoError(t, err)
	collectorB := newStateCollector("collector-b")
	collectorBPID, err := system.Spawn(collectorB)
	require.NoError(t, err)

	// Node A completes 2 rounds locally, node B completes 3, before ever gossiping.
	require.NoError(t, registryA.Tell(&registrypb.RoundComplete{NodeId: "node-a"}))
	require.NoError(t, registryA.Tell(&registrypb.RoundComplete{NodeId: "node-a"}))
	require.NoError(t, registryB.Tell(&registrypb.RoundComplete{NodeId: "node-b"}))
	require.NoError(t, registryB.Tell(&registrypb.RoundComplete{NodeId: "node-b"}))
	require.NoError(t, registryB.Tell(&registrypb.RoundComplete{NodeId: "node-b"}))

	// Fetch B's snapshot and gossip it to A, and vice versa.
	require.NoError(t, registryB.Tell(QueryState{ReplyTo: collectorBPID}))
	bState := awaitState(t, collectorB.states)
	require.NoError(t, registryA.Tell(QueryState{ReplyTo: collectorAPID}))
	aState := awaitState(t, collectorA.states)

	require.NoError(t, registryA.Tell(&registrypb.MergeRequest{NodeId: "node-b", Counter: toInt64Map(bState.Counter)}))
	require.NoError(t, registryB.Tell(&registrypb.MergeRequest{NodeId: "node-a", Counter: toInt64Map(aState.Counter)}))

	require.NoError(t, registryA.Tell(QueryState{ReplyTo: collectorAPID}))
	require.NoError(t, registryB.Tell(QueryState{ReplyTo: collectorBPID}))

	converged := awaitState(t, collectorA.states)
	convergedB := awaitState(t, collectorB.states)

	assert.Equal(t, 5, converged.CompletedRounds)
	assert.Equal(t, converged.Counter, convergedB.Counter)
}
