// Package fl implements the federated-learning demo actors (Worker,
// Aggregator, Coordinator) on top of the generic pkg/actor framework.
//
// TrainRequest, LocalModelUpdate, AggregateTrigger and AggregatedModel are
// generated protobuf types (see internal/fl/flpb) since they are the
// messages that cross real node boundaries when Worker, Aggregator and
// Coordinator run as separate processes or machines, carried over
// pkg/actor/remote's gRPC transport.
package fl

import "github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/nb"

// StartTraining kicks off one FL round: the Coordinator sends a TrainRequest
// to every worker listed in PartitionPaths (index i -> worker i) and tells
// the Aggregator how many updates to expect. It stays a plain, local-only
// message: it is only ever Tell'd to the Coordinator by the process that
// spawned it, never sent across the wire.
type StartTraining struct {
	PartitionPaths []string
}

// RoundResult is sent out-of-band (via a Go channel, not the actor mailbox)
// so the driver program can block until the Coordinator has evaluated a round.
type RoundResult struct {
	RoundID int
	Metrics nb.Metrics
}
