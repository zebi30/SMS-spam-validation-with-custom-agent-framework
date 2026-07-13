// Command fl-demo runs one round of federated Naive Bayes training for SMS
// spam detection and compares the result against a centrally trained
// baseline model, demonstrating the generic actor framework in pkg/actor.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/dataset"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/fl"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/nb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

func main() {
	dataPath := flag.String("data", "data/SMSSpamCollection", "path to the SMS Spam Collection TSV file")
	partitionDir := flag.String("partitions-dir", "data/partitions", "directory to write per-worker training partitions")
	numWorkers := flag.Int("workers", 3, "number of Worker actors (and dataset partitions)")
	trainRatio := flag.Float64("train-ratio", 0.8, "fraction of the dataset used for training")
	seed := flag.Int64("seed", 42, "random seed for the train/test split")
	flag.Parse()

	records, err := dataset.Load(*dataPath)
	if err != nil {
		log.Fatalf("load dataset: %v", err)
	}
	fmt.Printf("loaded %d messages from %s\n", len(records), *dataPath)

	train, test := dataset.SplitTrainTest(records, *trainRatio, *seed)
	fmt.Printf("split: %d train / %d test\n", len(train), len(test))

	baselineModel := nb.Aggregate([]nb.LocalCounts{nb.CountLocal(train)})
	baseline := nb.Evaluate(baselineModel, test)
	fmt.Printf("centralized baseline: accuracy=%.4f precision=%.4f recall=%.4f f1=%.4f\n",
		baseline.Accuracy, baseline.Precision, baseline.Recall, baseline.F1)

	partitionPaths, err := writePartitions(train, *numWorkers, *partitionDir)
	if err != nil {
		log.Fatalf("write partitions: %v", err)
	}

	flMetrics, err := runFederatedRound(partitionPaths, test)
	if err != nil {
		log.Fatalf("federated round: %v", err)
	}
	fmt.Printf("federated model:      accuracy=%.4f precision=%.4f recall=%.4f f1=%.4f\n",
		flMetrics.Accuracy, flMetrics.Precision, flMetrics.Recall, flMetrics.F1)

	relative := flMetrics.Accuracy / baseline.Accuracy
	status := "FAIL"
	if relative >= 0.95 {
		status = "PASS"
	}
	fmt.Printf("federated/baseline accuracy ratio: %.4f (target >= 0.95) [%s]\n", relative, status)
}

// writePartitions splits train into n shards and saves each as its own TSV
// file, since TrainRequest addresses a worker's data by path, not by value.
func writePartitions(train []dataset.Record, n int, dir string) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create partitions dir: %w", err)
	}

	shards := dataset.Partition(train, n)
	paths := make([]string, n)
	for i, shard := range shards {
		path := filepath.Join(dir, fmt.Sprintf("worker-%d.tsv", i))
		if err := dataset.Save(shard, path); err != nil {
			return nil, fmt.Errorf("save partition %d: %w", i, err)
		}
		paths[i] = path
	}
	return paths, nil
}

// runFederatedRound wires up the Coordinator, Aggregator and Worker actors,
// runs a single FL round over partitionPaths, and returns the resulting
// global model's evaluation metrics against test.
func runFederatedRound(partitionPaths []string, test []dataset.Record) (nb.Metrics, error) {
	system := actor.NewActorSystem()
	defer system.Shutdown()

	results := make(chan fl.RoundResult, 1)
	coordinator := &fl.CoordinatorActor{
		CoordinatorID: "coordinator",
		TestSet:       test,
		Results:       results,
	}
	coordinatorPID, err := system.Spawn(coordinator)
	if err != nil {
		return nb.Metrics{}, fmt.Errorf("spawn coordinator: %w", err)
	}

	aggregatorPID, err := system.Spawn(fl.NewAggregatorActor("aggregator", coordinatorPID))
	if err != nil {
		return nb.Metrics{}, fmt.Errorf("spawn aggregator: %w", err)
	}

	workerPIDs := make([]actor.Ref, len(partitionPaths))
	for i := range partitionPaths {
		worker := &fl.WorkerActor{
			WorkerID:   fmt.Sprintf("worker-%d", i),
			Aggregator: aggregatorPID,
		}
		pid, err := system.Spawn(worker)
		if err != nil {
			return nb.Metrics{}, fmt.Errorf("spawn %s: %w", worker.WorkerID, err)
		}
		workerPIDs[i] = pid
	}

	// Safe without synchronization: these fields are only read once the
	// coordinator's goroutine processes StartTraining below, and the Tell
	// call's channel send happens-before that read.
	coordinator.Aggregator = aggregatorPID
	coordinator.Workers = workerPIDs

	if err := coordinatorPID.Tell(fl.StartTraining{PartitionPaths: partitionPaths}); err != nil {
		return nb.Metrics{}, fmt.Errorf("start training: %w", err)
	}

	select {
	case result := <-results:
		return result.Metrics, nil
	case <-time.After(30 * time.Second):
		return nb.Metrics{}, fmt.Errorf("timed out waiting for FL round to complete")
	}
}
