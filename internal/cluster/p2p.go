package cluster

import (
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/cluster/clusterpb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

// peerJoined and peerLeft are fed into the owning P2PActor's own mailbox
// from memberlist's background goroutines (via its PID, a thread-safe
// channel send), so peer-map mutation stays confined to the actor's single
// run-loop goroutine like every other actor in this framework.
type peerJoined struct{ info *clusterpb.PeerInfo }
type peerLeft struct{ nodeID string }

// P2PActor is a ClusterActor that discovers peers via gossip
// (hashicorp/memberlist) instead of a central Provider node: no single
// point of failure, at the cost of eventually- rather than immediately-
// consistent membership views.
type P2PActor struct {
	SelfID        string   // memberlist node name and actor ID
	BindAddr      string   // host:port memberlist itself gossips on
	TransportAddr string   // host:port of this node's actor-transport gRPC server, advertised to peers
	Seeds         []string // known peer BindAddrs to join through; empty for the first node

	list  *memberlist.Memberlist
	peers map[string]*clusterpb.PeerInfo
	self  *actor.PID
}

// NewP2PActor builds a P2P-mode ClusterActor. Seeds may be empty for the
// first node in a cluster; later nodes join through any already-running peer.
func NewP2PActor(selfID, bindAddr, transportAddr string, seeds []string) *P2PActor {
	return &P2PActor{
		SelfID:        selfID,
		BindAddr:      bindAddr,
		TransportAddr: transportAddr,
		Seeds:         seeds,
		peers:         make(map[string]*clusterpb.PeerInfo),
	}
}

func (p *P2PActor) ID() string { return p.SelfID }

func (p *P2PActor) OnStart(ctx *actor.Context) {
	// Set before touching memberlist at all: Join can synchronously trigger
	// NotifyJoin callbacks for already-known peers.
	p.self = ctx.Self()

	host, portStr, err := net.SplitHostPort(p.BindAddr)
	if err != nil {
		log.Printf("cluster: p2p node %s: invalid bind addr %q: %v", p.SelfID, p.BindAddr, err)
		return
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Printf("cluster: p2p node %s: invalid bind port %q: %v", p.SelfID, portStr, err)
		return
	}

	config := memberlist.DefaultLocalConfig()
	config.Name = p.SelfID
	config.BindAddr = host
	config.BindPort = port
	config.AdvertiseAddr = host
	config.AdvertisePort = port
	config.Delegate = &p2pDelegate{transportAddr: p.TransportAddr}
	config.Events = &p2pEvents{owner: p}
	config.LogOutput = io.Discard

	list, err := memberlist.Create(config)
	if err != nil {
		log.Printf("cluster: p2p node %s: create memberlist: %v", p.SelfID, err)
		return
	}
	p.list = list

	if len(p.Seeds) > 0 {
		joinWithRetry(p.SelfID, list, p.Seeds)
	}
}

// joinWithRetry retries memberlist.Join a few times with a short delay, so a
// seed that is still starting up (e.g. a Docker container not yet listening)
// doesn't permanently exclude this node from the cluster.
func joinWithRetry(selfID string, list *memberlist.Memberlist, seeds []string) {
	const attempts = 10
	const delay = 500 * time.Millisecond

	var err error
	for i := 0; i < attempts; i++ {
		if _, err = list.Join(seeds); err == nil {
			return
		}
		if i < attempts-1 {
			time.Sleep(delay)
		}
	}
	log.Printf("cluster: p2p node %s: join seeds %v: %v", selfID, seeds, err)
}

// LocalAddr returns the host:port memberlist actually bound to. It is only
// meaningful to call after Spawn has returned (OnStart runs synchronously as
// part of it), and is most useful when BindAddr specified port 0 for an
// OS-assigned ephemeral port that other nodes need to be told about.
func (p *P2PActor) LocalAddr() string {
	if p.list == nil {
		return ""
	}
	node := p.list.LocalNode()
	return net.JoinHostPort(node.Addr.String(), strconv.Itoa(int(node.Port)))
}

func (p *P2PActor) OnStop(_ *actor.Context) {
	if p.list == nil {
		return
	}
	_ = p.list.Leave(2 * time.Second)
	_ = p.list.Shutdown()
}

func (p *P2PActor) Receive(ctx *actor.Context, msg actor.Message) {
	switch m := msg.(type) {
	case peerJoined:
		p.peers[m.info.GetNodeId()] = m.info
	case peerLeft:
		delete(p.peers, m.nodeID)
	case QueryPeers:
		p.replyPeers(ctx, m)
	}
}

func (p *P2PActor) replyPeers(ctx *actor.Context, q QueryPeers) {
	if q.ReplyTo == nil {
		return
	}
	peers := make([]*clusterpb.PeerInfo, 0, len(p.peers))
	for _, info := range p.peers {
		peers = append(peers, info)
	}
	_ = ctx.Send(q.ReplyTo, PeersView{Peers: peers})
}

// p2pEvents bridges memberlist's background-goroutine membership callbacks
// into the owning actor's mailbox rather than mutating shared state directly.
type p2pEvents struct{ owner *P2PActor }

func (e *p2pEvents) NotifyJoin(n *memberlist.Node) {
	if e.owner.self == nil || n.Name == e.owner.SelfID {
		return
	}
	_ = e.owner.self.Tell(peerJoined{info: &clusterpb.PeerInfo{NodeId: n.Name, Addr: string(n.Meta)}})
}

func (e *p2pEvents) NotifyLeave(n *memberlist.Node) {
	if e.owner.self == nil {
		return
	}
	_ = e.owner.self.Tell(peerLeft{nodeID: n.Name})
}

func (e *p2pEvents) NotifyUpdate(n *memberlist.Node) {
	e.NotifyJoin(n) // a metadata update is treated just like a (re-)join
}

// p2pDelegate advertises this node's actor-transport gRPC address as gossip
// metadata so peers learn how to reach it, without needing a central registry.
type p2pDelegate struct{ transportAddr string }

func (d *p2pDelegate) NodeMeta(limit int) []byte {
	b := []byte(d.transportAddr)
	if len(b) > limit {
		b = b[:limit]
	}
	return b
}

func (d *p2pDelegate) NotifyMsg([]byte) {}

func (d *p2pDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

func (d *p2pDelegate) LocalState(join bool) []byte { return nil }

func (d *p2pDelegate) MergeRemoteState(buf []byte, join bool) {}
