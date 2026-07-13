package fl

import (
	"log"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/dataset"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/fl/flpb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/nb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

// CoordinatorActor orchestrates FL rounds and evaluates the resulting global
// model against a test set that never leaves this actor. Workers and
// Aggregator may be local *actor.PID or remote.Ref, so the same code
// orchestrates a round whether every actor lives in one process or each
// runs on its own machine.
type CoordinatorActor struct {
	CoordinatorID string
	Workers       []actor.Ref
	Aggregator    actor.Ref
	TestSet       []dataset.Record
	Results       chan RoundResult

	roundID int64
}

func (c *CoordinatorActor) ID() string { return c.CoordinatorID }

func (c *CoordinatorActor) Receive(ctx *actor.Context, msg actor.Message) {
	switch m := msg.(type) {
	case StartTraining:
		c.startRound(ctx, m)
	case *flpb.AggregatedModel:
		c.evaluate(m)
	}
}

func (c *CoordinatorActor) startRound(ctx *actor.Context, start StartTraining) {
	if len(start.PartitionPaths) != len(c.Workers) {
		log.Printf("coordinator %s: got %d partitions for %d workers", c.CoordinatorID, len(start.PartitionPaths), len(c.Workers))
		return
	}

	c.roundID++
	roundID := c.roundID

	trigger := &flpb.AggregateTrigger{RoundId: roundID, ExpectedWorkers: int64(len(c.Workers))}
	if err := sendWithRetry(ctx, c.Aggregator, trigger); err != nil {
		log.Printf("coordinator %s: round %d: trigger aggregator: %v", c.CoordinatorID, roundID, err)
	}

	for i, worker := range c.Workers {
		req := &flpb.TrainRequest{RoundId: roundID, DatasetPartitionPath: start.PartitionPaths[i]}
		if err := sendWithRetry(ctx, worker, req); err != nil {
			log.Printf("coordinator %s: round %d: send to worker %s: %v", c.CoordinatorID, roundID, worker.ID(), err)
		}
	}
}

func (c *CoordinatorActor) evaluate(m *flpb.AggregatedModel) {
	model := nb.GlobalModel{
		WordProbs:   fromPBWordProbs(m.GetGlobalWordProbs()),
		ClassPriors: m.GetClassPriors(),
	}
	for _, probs := range model.WordProbs {
		model.VocabSize = len(probs)
		break
	}

	metrics := nb.Evaluate(model, c.TestSet)
	log.Printf("coordinator %s: round %d evaluation: accuracy=%.4f precision=%.4f recall=%.4f f1=%.4f",
		c.CoordinatorID, m.GetRoundId(), metrics.Accuracy, metrics.Precision, metrics.Recall, metrics.F1)

	if c.Results != nil {
		c.Results <- RoundResult{RoundID: int(m.GetRoundId()), Metrics: metrics}
	}
}
