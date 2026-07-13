package cluster_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/cluster"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

// TestP2PModeDiscoversPeersViaGossip spins up two P2PActor nodes, each with
// its own memberlist instance bound to its own ephemeral UDP/TCP port on
// localhost. Node B seeds off node A; both must learn about each other
// purely through gossip, with no central registry involved.
func TestP2PModeDiscoversPeersViaGossip(t *testing.T) {
	systemA := actor.NewActorSystem()
	defer systemA.Shutdown()

	nodeA := cluster.NewP2PActor("node-a", "127.0.0.1:0", "127.0.0.1:19101", nil)
	_, err := systemA.Spawn(nodeA)
	require.NoError(t, err)
	addrA := nodeA.LocalAddr()
	require.NotEmpty(t, addrA)

	systemB := actor.NewActorSystem()
	defer systemB.Shutdown()

	nodeB := cluster.NewP2PActor("node-b", "127.0.0.1:0", "127.0.0.1:19102", []string{addrA})
	_, err = systemB.Spawn(nodeB)
	require.NoError(t, err)

	pidA, ok := systemA.Lookup("node-a")
	require.True(t, ok)
	pidB, ok := systemB.Lookup("node-b")
	require.True(t, ok)

	collectorA := &collector{id: "collector-a", view: make(chan cluster.PeersView, 4)}
	_, err = systemA.Spawn(collectorA)
	require.NoError(t, err)
	collectorB := &collector{id: "collector-b", view: make(chan cluster.PeersView, 4)}
	_, err = systemB.Spawn(collectorB)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ids, ok := tryPeerIDs(systemA, pidA, collectorA)
		return ok && containsAll(ids, "node-b")
	}, 5*time.Second, 100*time.Millisecond, "node-a never discovered node-b via gossip")

	require.Eventually(t, func() bool {
		ids, ok := tryPeerIDs(systemB, pidB, collectorB)
		return ok && containsAll(ids, "node-a")
	}, 5*time.Second, 100*time.Millisecond, "node-b never discovered node-a via gossip")

	// The gossiped metadata must carry each node's actor-transport address,
	// not just its memberlist identity, since that's what a real ClusterActor
	// would use to address MergeRequest/TrainRequest traffic to that peer.
	require.NoError(t, pidA.Tell(cluster.QueryPeers{ReplyTo: mustLookupPID(t, systemA, "collector-a")}))
	view := awaitPeersView(t, collectorA.view)
	require.Len(t, view.Peers, 1)
	assert.Equal(t, "node-b", view.Peers[0].GetNodeId())
	assert.Equal(t, "127.0.0.1:19102", view.Peers[0].GetAddr())
}

func mustLookupPID(t *testing.T, system *actor.ActorSystem, id string) *actor.PID {
	t.Helper()
	pid, ok := system.Lookup(id)
	require.True(t, ok)
	return pid
}

func awaitPeersView(t *testing.T, ch chan cluster.PeersView) cluster.PeersView {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for PeersView")
		return cluster.PeersView{}
	}
}
