package remote_test

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/registry"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/registry/registrypb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote/remotepb"
)

// node bundles everything one simulated cluster node needs: its own
// ActorSystem, its own gRPC server exposing that system's Transport, and the
// listener address peers dial to reach it.
type node struct {
	system     *actor.ActorSystem
	grpcServer *grpc.Server
	addr       string
}

func startNode(t *testing.T) *node {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	system := actor.NewActorSystem()
	grpcServer := grpc.NewServer()
	remotepb.RegisterTransportServer(grpcServer, remote.NewServer(system))

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	return &node{system: system, grpcServer: grpcServer, addr: lis.Addr().String()}
}

func (n *node) stop() {
	n.grpcServer.Stop()
	n.system.Shutdown()
}

func dialTransport(t *testing.T, addr string) remotepb.TransportClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return remotepb.NewTransportClient(conn)
}

// stateCollector receives registry.RegistryState replies for the test to inspect.
type stateCollector struct {
	id     string
	states chan registry.RegistryState
}

func (c *stateCollector) ID() string { return c.id }

func (c *stateCollector) Receive(_ *actor.Context, msg actor.Message) {
	if state, ok := msg.(registry.RegistryState); ok {
		c.states <- state
	}
}

func awaitRegistryState(t *testing.T, ch chan registry.RegistryState) registry.RegistryState {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RegistryState")
		return registry.RegistryState{}
	}
}

// TestMergeRequestOverRealGRPC proves the generic transport actually works
// across a real network boundary: two RegistryActors, each owned by its own
// ActorSystem and gRPC server on its own TCP port, gossip MergeRequest
// protobuf messages to each other and converge to the same G-Counter value
// exactly as the in-process registry test does purely locally.
func TestMergeRequestOverRealGRPC(t *testing.T) {
	nodeA := startNode(t)
	defer nodeA.stop()
	nodeB := startNode(t)
	defer nodeB.stop()

	_, err := nodeA.system.Spawn(registry.NewRegistryActor("registry-a"))
	require.NoError(t, err)
	_, err = nodeB.system.Spawn(registry.NewRegistryActor("registry-b"))
	require.NoError(t, err)

	registryAPID, ok := nodeA.system.Lookup("registry-a")
	require.True(t, ok)
	registryBPID, ok := nodeB.system.Lookup("registry-b")
	require.True(t, ok)

	collectorA := &stateCollector{id: "collector-a", states: make(chan registry.RegistryState, 4)}
	collectorAPID, err := nodeA.system.Spawn(collectorA)
	require.NoError(t, err)
	collectorB := &stateCollector{id: "collector-b", states: make(chan registry.RegistryState, 4)}
	collectorBPID, err := nodeB.system.Spawn(collectorB)
	require.NoError(t, err)

	// Node A completes 2 rounds locally, node B completes 3 - purely local Tells.
	require.NoError(t, registryAPID.Tell(&registrypb.RoundComplete{NodeId: "node-a"}))
	require.NoError(t, registryAPID.Tell(&registrypb.RoundComplete{NodeId: "node-a"}))
	require.NoError(t, registryBPID.Tell(&registrypb.RoundComplete{NodeId: "node-b"}))
	require.NoError(t, registryBPID.Tell(&registrypb.RoundComplete{NodeId: "node-b"}))
	require.NoError(t, registryBPID.Tell(&registrypb.RoundComplete{NodeId: "node-b"}))

	require.NoError(t, registryAPID.Tell(registry.QueryState{ReplyTo: collectorAPID}))
	aState := awaitRegistryState(t, collectorA.states)
	require.NoError(t, registryBPID.Tell(registry.QueryState{ReplyTo: collectorBPID}))
	bState := awaitRegistryState(t, collectorB.states)

	// Now gossip each side's snapshot to the other over a real gRPC connection.
	refToB := &remote.Ref{TargetID: "registry-b", Client: dialTransport(t, nodeB.addr)}
	refToA := &remote.Ref{TargetID: "registry-a", Client: dialTransport(t, nodeA.addr)}

	require.NoError(t, refToB.Tell(&registrypb.MergeRequest{NodeId: "node-a", Counter: toInt64Map(aState.Counter)}))
	require.NoError(t, refToA.Tell(&registrypb.MergeRequest{NodeId: "node-b", Counter: toInt64Map(bState.Counter)}))

	require.NoError(t, registryAPID.Tell(registry.QueryState{ReplyTo: collectorAPID}))
	converged := awaitRegistryState(t, collectorA.states)
	require.NoError(t, registryBPID.Tell(registry.QueryState{ReplyTo: collectorBPID}))
	convergedB := awaitRegistryState(t, collectorB.states)

	assert.Equal(t, 5, converged.CompletedRounds)
	assert.Equal(t, converged.Counter, convergedB.Counter)
}

func toInt64Map(m map[string]int) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = int64(v)
	}
	return out
}
