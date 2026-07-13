package cluster

import (
	"log"

	"google.golang.org/grpc"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/cluster/clusterpb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote"
)

// ProviderClientActor is a regular cluster node in Provider mode: it
// registers with a central ProviderActor and learns the peer list from it.
//
// The node's own actor-transport gRPC server must already be listening on
// SelfAddr before this actor is spawned, since the Provider's PeerList reply
// is delivered by dialing back into SelfAddr as soon as OnStart runs.
type ProviderClientActor struct {
	SelfID       string
	SelfAddr     string
	ProviderID   string
	ProviderAddr string

	peers    map[string]*clusterpb.PeerInfo
	conn     *grpc.ClientConn
	provider *remote.Ref
}

// NewProviderClientActor builds a client-mode ClusterActor for selfID,
// reachable at selfAddr, that registers with the Provider node providerID at providerAddr.
func NewProviderClientActor(selfID, selfAddr, providerID, providerAddr string) *ProviderClientActor {
	return &ProviderClientActor{
		SelfID:       selfID,
		SelfAddr:     selfAddr,
		ProviderID:   providerID,
		ProviderAddr: providerAddr,
		peers:        make(map[string]*clusterpb.PeerInfo),
	}
}

func (c *ProviderClientActor) ID() string { return c.SelfID }

// OnStart dials the Provider once and keeps the connection open for the
// actor's lifetime: re-dialing on every registerAndDiscover call would pay a
// full connection handshake each time instead of reusing a warm one.
func (c *ProviderClientActor) OnStart(_ *actor.Context) {
	ref, conn, err := remote.NewRef(c.ProviderAddr, c.ProviderID)
	if err != nil {
		log.Printf("cluster: node %s: dial provider %s: %v", c.SelfID, c.ProviderAddr, err)
		return
	}
	c.conn = conn
	c.provider = ref
	c.registerAndDiscover()
}

func (c *ProviderClientActor) OnStop(_ *actor.Context) {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *ProviderClientActor) Receive(ctx *actor.Context, msg actor.Message) {
	switch m := msg.(type) {
	case *clusterpb.PeerList:
		c.peers = make(map[string]*clusterpb.PeerInfo, len(m.GetPeers()))
		for _, peer := range m.GetPeers() {
			c.peers[peer.GetNodeId()] = peer
		}
	case DiscoverNow:
		c.registerAndDiscover()
	case QueryPeers:
		c.replyPeers(ctx, m)
	}
}

func (c *ProviderClientActor) registerAndDiscover() {
	if c.provider == nil {
		return // initial dial in OnStart failed; nothing to retry against
	}

	self := &clusterpb.PeerInfo{NodeId: c.SelfID, Addr: c.SelfAddr}
	if err := c.provider.Tell(&clusterpb.JoinCluster{Self: self}); err != nil {
		log.Printf("cluster: node %s: join: %v", c.SelfID, err)
		return
	}

	replyTo := &clusterpb.ReplyAddress{Addr: c.SelfAddr, ActorId: c.SelfID}
	if err := c.provider.Tell(&clusterpb.DiscoverPeers{ReplyTo: replyTo}); err != nil {
		log.Printf("cluster: node %s: discover: %v", c.SelfID, err)
	}
}

func (c *ProviderClientActor) replyPeers(ctx *actor.Context, q QueryPeers) {
	if q.ReplyTo == nil {
		return
	}
	peers := make([]*clusterpb.PeerInfo, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, p)
	}
	_ = ctx.Send(q.ReplyTo, PeersView{Peers: peers})
}
