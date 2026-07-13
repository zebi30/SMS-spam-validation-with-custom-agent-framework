package cluster

import (
	"log"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/cluster/clusterpb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor/remote/remotepb"
)

// ProviderActor is the central registry node in Provider cluster mode:
// peers register with it via JoinCluster and learn about each other via
// DiscoverPeers. There is no gossip and no peer-to-peer discovery - simpler
// to operate, at the cost of the provider being a single point of failure.
type ProviderActor struct {
	ProviderID string

	peers map[string]*clusterpb.PeerInfo
}

// NewProviderActor builds an empty Provider registry identified by id.
func NewProviderActor(id string) *ProviderActor {
	return &ProviderActor{ProviderID: id, peers: make(map[string]*clusterpb.PeerInfo)}
}

func (p *ProviderActor) ID() string { return p.ProviderID }

func (p *ProviderActor) Receive(ctx *actor.Context, msg actor.Message) {
	switch m := msg.(type) {
	case *clusterpb.JoinCluster:
		if self := m.GetSelf(); self != nil {
			p.peers[self.GetNodeId()] = self
		}
	case *clusterpb.LeaveCluster:
		delete(p.peers, m.GetNodeId())
	case *clusterpb.DiscoverPeers:
		p.replyPeerList(m.GetReplyTo())
	}
}

func (p *ProviderActor) replyPeerList(to *clusterpb.ReplyAddress) {
	if to == nil || to.GetAddr() == "" {
		return
	}

	peers := make([]*clusterpb.PeerInfo, 0, len(p.peers))
	for _, info := range p.peers {
		peers = append(peers, info)
	}

	conn, err := remote.Dial(to.GetAddr())
	if err != nil {
		log.Printf("cluster: provider %s: dial %s: %v", p.ProviderID, to.GetAddr(), err)
		return
	}
	defer conn.Close()

	ref := &remote.Ref{TargetID: to.GetActorId(), Client: remotepb.NewTransportClient(conn)}
	if err := ref.Tell(&clusterpb.PeerList{Peers: peers}); err != nil {
		log.Printf("cluster: provider %s: reply to %s: %v", p.ProviderID, to.GetActorId(), err)
	}
}
