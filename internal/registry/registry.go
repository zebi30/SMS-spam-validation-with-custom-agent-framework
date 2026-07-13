package registry

import (
	"sort"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/crdt"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/registry/registrypb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

// RegistryActor tracks active cluster nodes and completed FL rounds. Round
// counts are stored in a G-Counter so that multiple registry replicas (e.g.
// one per node in P2P mode) can be merged into a consistent view without a
// single point of coordination.
type RegistryActor struct {
	RegistryID string

	counter *crdt.GCounter
	active  map[string]struct{}
}

// NewRegistryActor builds an empty registry identified by id.
func NewRegistryActor(id string) *RegistryActor {
	return &RegistryActor{
		RegistryID: id,
		counter:    crdt.NewGCounter(),
		active:     make(map[string]struct{}),
	}
}

func (r *RegistryActor) ID() string { return r.RegistryID }

func (r *RegistryActor) Receive(ctx *actor.Context, msg actor.Message) {
	switch m := msg.(type) {
	case *registrypb.NodeJoin:
		r.active[m.GetNodeId()] = struct{}{}
	case *registrypb.NodeLeave:
		delete(r.active, m.GetNodeId())
	case *registrypb.RoundComplete:
		r.counter.Increment(m.GetNodeId(), 1)
	case *registrypb.MergeRequest:
		r.counter.Merge(crdt.FromSnapshot(toIntMap(m.GetCounter())))
	case QueryState:
		r.replyState(ctx, m)
	}
}

func (r *RegistryActor) replyState(ctx *actor.Context, q QueryState) {
	if q.ReplyTo == nil {
		return
	}
	nodes := make([]string, 0, len(r.active))
	for id := range r.active {
		nodes = append(nodes, id)
	}
	sort.Strings(nodes)

	_ = ctx.Send(q.ReplyTo, RegistryState{
		ActiveNodes:     nodes,
		CompletedRounds: r.counter.Value(),
		Counter:         r.counter.Snapshot(),
	})
}

func toInt64Map(m map[string]int) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = int64(v)
	}
	return out
}

func toIntMap(m map[string]int64) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = int(v)
	}
	return out
}
