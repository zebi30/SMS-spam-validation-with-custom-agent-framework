// Package registry implements RegistryActor: cluster membership and
// completed-round tracking via a G-Counter CRDT, so state converges without
// central coordination even when nodes only ever see partial gossip.
//
// NodeJoin, NodeLeave, RoundComplete and MergeRequest are generated protobuf
// types (see internal/registry/registrypb) since they are the messages that
// cross real node boundaries in Provider and P2P cluster mode, carried over
// pkg/actor/remote's gRPC transport.
package registry

import "github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"

// QueryState asks the registry to report its current view back to ReplyTo.
// It stays a plain, local-only message: ReplyTo is an in-process PID and
// cannot be serialized to a remote peer.
type QueryState struct {
	ReplyTo *actor.PID
}

// RegistryState is the registry's answer to QueryState.
type RegistryState struct {
	ActiveNodes     []string
	CompletedRounds int
	Counter         map[string]int
}
