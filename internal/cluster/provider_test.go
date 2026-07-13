package cluster_test

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/cluster"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote/remotepb"
)

// testNode bundles an ActorSystem with its own gRPC transport server, one
// per simulated cluster node, exactly like a real deployment where every
// process runs both.
type testNode struct {
	system     *actor.ActorSystem
	grpcServer *grpc.Server
	addr       string
}

func startTestNode(t *testing.T) *testNode {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	system := actor.NewActorSystem()
	grpcServer := grpc.NewServer()
	remotepb.RegisterTransportServer(grpcServer, remote.NewServer(system))
	go func() { _ = grpcServer.Serve(lis) }()

	return &testNode{system: system, grpcServer: grpcServer, addr: lis.Addr().String()}
}

func (n *testNode) stop() {
	n.grpcServer.Stop()
	n.system.Shutdown()
}

// collector receives PeersView replies for the test to inspect.
type collector struct {
	id   string
	view chan cluster.PeersView
}

func (c *collector) ID() string { return c.id }

func (c *collector) Receive(_ *actor.Context, msg actor.Message) {
	if view, ok := msg.(cluster.PeersView); ok {
		c.view <- view
	}
}

// tryPeerIDs is safe to call from testify's Eventually, which runs the
// condition function on its own goroutine: it never calls t.Fatal/require,
// only returns ok=false on any failure so the poll loop can just retry.
func tryPeerIDs(system *actor.ActorSystem, clusterPID *actor.PID, col *collector) (ids []string, ok bool) {
	collectorPID, found := system.Lookup(col.id)
	if !found {
		return nil, false
	}
	if err := clusterPID.Tell(cluster.QueryPeers{ReplyTo: collectorPID}); err != nil {
		return nil, false
	}
	select {
	case view := <-col.view:
		ids = make([]string, len(view.Peers))
		for i, p := range view.Peers {
			ids[i] = p.GetNodeId()
		}
		return ids, true
	case <-time.After(500 * time.Millisecond):
		return nil, false
	}
}

func peerIDs(t *testing.T, system *actor.ActorSystem, clusterPID *actor.PID, col *collector) []string {
	t.Helper()
	ids, ok := tryPeerIDs(system, clusterPID, col)
	require.True(t, ok, "timed out waiting for PeersView")
	return ids
}

// TestProviderModeDiscoversPeers spins up one ProviderActor and two
// ProviderClientActor nodes, each a real process stand-in with its own
// ActorSystem and gRPC server on its own TCP port. Both clients register
// with the provider and must learn about each other purely through it -
// there is no direct client-to-client communication in Provider mode.
func TestProviderModeDiscoversPeers(t *testing.T) {
	providerNode := startTestNode(t)
	defer providerNode.stop()
	_, err := providerNode.system.Spawn(cluster.NewProviderActor("provider"))
	require.NoError(t, err)

	nodeA := startTestNode(t)
	defer nodeA.stop()
	collectorA := &collector{id: "collector-a", view: make(chan cluster.PeersView, 4)}
	_, err = nodeA.system.Spawn(collectorA)
	require.NoError(t, err)
	clientAPID, err := nodeA.system.Spawn(cluster.NewProviderClientActor("node-a", nodeA.addr, "provider", providerNode.addr))
	require.NoError(t, err)

	nodeB := startTestNode(t)
	defer nodeB.stop()
	collectorB := &collector{id: "collector-b", view: make(chan cluster.PeersView, 4)}
	_, err = nodeB.system.Spawn(collectorB)
	require.NoError(t, err)
	clientBPID, err := nodeB.system.Spawn(cluster.NewProviderClientActor("node-b", nodeB.addr, "provider", providerNode.addr))
	require.NoError(t, err)

	// Node A joined before node B existed, so it must ask again to learn
	// about B once B has also registered. The poll interval is deliberately
	// coarse: each DiscoverNow opens a fresh gRPC connection, and Ref.Tell
	// blocks with WaitForReady, so a tight loop would pile up far more
	// concurrent dials than this is meant to exercise.
	require.Eventually(t, func() bool {
		if err := clientBPID.Tell(cluster.DiscoverNow{}); err != nil {
			return false
		}
		if err := clientAPID.Tell(cluster.DiscoverNow{}); err != nil {
			return false
		}
		ids, ok := tryPeerIDs(nodeA.system, clientAPID, collectorA)
		return ok && containsAll(ids, "node-a", "node-b")
	}, 5*time.Second, 250*time.Millisecond)

	idsA := peerIDs(t, nodeA.system, clientAPID, collectorA)
	idsB := peerIDs(t, nodeB.system, clientBPID, collectorB)
	assert.ElementsMatch(t, []string{"node-a", "node-b"}, idsA)
	assert.ElementsMatch(t, []string{"node-a", "node-b"}, idsB)
}

func containsAll(haystack []string, needles ...string) bool {
	set := make(map[string]struct{}, len(haystack))
	for _, h := range haystack {
		set[h] = struct{}{}
	}
	for _, n := range needles {
		if _, ok := set[n]; !ok {
			return false
		}
	}
	return true
}
