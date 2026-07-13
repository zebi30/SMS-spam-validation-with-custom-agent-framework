package fl

import (
	"log"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/fl/flpb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/nb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

// AggregatorActor collects per-round LocalModelUpdate messages from every
// Worker and, once all expected updates for a round have arrived, produces
// the FedAvg-aggregated global model for the Coordinator. Coordinator may be
// a local *actor.PID or a remote.Ref.
type AggregatorActor struct {
	AggregatorID string
	Coordinator  actor.Ref

	updates  map[int64][]nb.LocalCounts
	expected map[int64]int64
}

// NewAggregatorActor builds an AggregatorActor that reports results to coordinator.
func NewAggregatorActor(id string, coordinator actor.Ref) *AggregatorActor {
	return &AggregatorActor{
		AggregatorID: id,
		Coordinator:  coordinator,
		updates:      make(map[int64][]nb.LocalCounts),
		expected:     make(map[int64]int64),
	}
}

func (a *AggregatorActor) ID() string { return a.AggregatorID }

func (a *AggregatorActor) Receive(ctx *actor.Context, msg actor.Message) {
	switch m := msg.(type) {
	case *flpb.AggregateTrigger:
		a.expected[m.GetRoundId()] = m.GetExpectedWorkers()
		a.maybeAggregate(ctx, m.GetRoundId())
	case *flpb.LocalModelUpdate:
		roundID := m.GetRoundId()
		a.updates[roundID] = append(a.updates[roundID], nb.LocalCounts{
			WordCounts:  fromPBWordCounts(m.GetWordCounts()),
			ClassTotals: fromPBInt64Map(m.GetClassTotals()),
		})
		a.maybeAggregate(ctx, roundID)
	}
}

// maybeAggregate is safe to call in any order relative to AggregateTrigger vs
// LocalModelUpdate arrival, since both paths funnel through it.
func (a *AggregatorActor) maybeAggregate(ctx *actor.Context, roundID int64) {
	expected, known := a.expected[roundID]
	if !known || int64(len(a.updates[roundID])) < expected {
		return
	}

	model := nb.Aggregate(a.updates[roundID])
	delete(a.updates, roundID)
	delete(a.expected, roundID)

	err := sendWithRetry(ctx, a.Coordinator, &flpb.AggregatedModel{
		RoundId:         roundID,
		GlobalWordProbs: toPBWordProbs(model.WordProbs),
		ClassPriors:     model.ClassPriors,
	})
	if err != nil {
		log.Printf("aggregator %s: round %d: send to coordinator: %v", a.AggregatorID, roundID, err)
	}
}
