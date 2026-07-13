// Package cluster implements ClusterActor for both cluster modes described
// in the spec: a centralized Provider (ProviderActor + ProviderClientActor)
// and a peer-to-peer gossip mode (P2PActor, backed by hashicorp/memberlist).
// JoinCluster, LeaveCluster, DiscoverPeers and PeerList (see clusterpb) are
// the messages that cross real node boundaries in Provider mode, carried
// over pkg/actor/remote's gRPC transport.
package cluster

import (
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/cluster/clusterpb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

// DiscoverNow triggers an immediate (re-)registration and peer discovery.
// It is a local, in-process trigger, not something sent across the wire.
type DiscoverNow struct{}

// QueryPeers asks a cluster actor to report its current known peers back to ReplyTo.
type QueryPeers struct {
	ReplyTo *actor.PID
}

// PeersView is the answer to QueryPeers.
type PeersView struct {
	Peers []*clusterpb.PeerInfo
}
