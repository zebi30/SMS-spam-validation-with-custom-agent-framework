package fl

import (
	"log"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/dataset"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/fl/flpb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/nb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

// WorkerActor trains a local Naive Bayes count model on its own dataset
// partition and never shares the raw messages it was trained on, only the
// resulting word/class counts. Aggregator may be a local *actor.PID or a
// remote.Ref, so the same code runs whether Worker and Aggregator share a
// process or run on separate machines.
type WorkerActor struct {
	WorkerID   string
	Aggregator actor.Ref
}

func (w *WorkerActor) ID() string { return w.WorkerID }

func (w *WorkerActor) Receive(ctx *actor.Context, msg actor.Message) {
	req, ok := msg.(*flpb.TrainRequest)
	if !ok {
		return
	}

	records, err := dataset.Load(req.GetDatasetPartitionPath())
	if err != nil {
		log.Printf("worker %s: round %d: %v", w.WorkerID, req.GetRoundId(), err)
		return
	}

	counts := nb.CountLocal(records)
	update := &flpb.LocalModelUpdate{
		RoundId:     req.GetRoundId(),
		WorkerId:    w.WorkerID,
		WordCounts:  toPBWordCounts(counts.WordCounts),
		ClassTotals: toPBInt64Map(counts.ClassTotals),
	}
	if err := sendWithRetry(ctx, w.Aggregator, update); err != nil {
		log.Printf("worker %s: round %d: send to aggregator: %v", w.WorkerID, req.GetRoundId(), err)
	}
}
