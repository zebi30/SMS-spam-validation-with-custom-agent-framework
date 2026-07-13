package fl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/dataset"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/nb"
	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/pkg/actor"
)

func TestFederatedRoundMatchesDirectAggregation(t *testing.T) {
	train := []dataset.Record{
		{Label: dataset.LabelHam, Text: "hello friend how are you"},
		{Label: dataset.LabelHam, Text: "are we meeting today"},
		{Label: dataset.LabelSpam, Text: "win free money now"},
		{Label: dataset.LabelSpam, Text: "claim your free prize now"},
	}
	test := []dataset.Record{
		{Label: dataset.LabelHam, Text: "hello friend"},
		{Label: dataset.LabelSpam, Text: "win free prize now"},
	}

	// Two workers, one shard each, written to disk exactly like the real demo.
	dir := t.TempDir()
	shards := dataset.Partition(train, 2)
	paths := make([]string, len(shards))
	for i, shard := range shards {
		path := filepath.Join(dir, "worker-"+string(rune('0'+i))+".tsv")
		require.NoError(t, dataset.Save(shard, path))
		paths[i] = path
	}

	system := actor.NewActorSystem()
	defer system.Shutdown()

	results := make(chan RoundResult, 1)
	coordinator := &CoordinatorActor{
		CoordinatorID: "coordinator",
		TestSet:       test,
		Results:       results,
	}
	coordinatorPID, err := system.Spawn(coordinator)
	require.NoError(t, err)

	aggregatorPID, err := system.Spawn(NewAggregatorActor("aggregator", coordinatorPID))
	require.NoError(t, err)

	workerPIDs := make([]actor.Ref, len(paths))
	for i := range paths {
		w := &WorkerActor{WorkerID: "worker-" + string(rune('0'+i)), Aggregator: aggregatorPID}
		pid, err := system.Spawn(w)
		require.NoError(t, err)
		workerPIDs[i] = pid
	}
	coordinator.Aggregator = aggregatorPID
	coordinator.Workers = workerPIDs

	require.NoError(t, coordinatorPID.Tell(StartTraining{PartitionPaths: paths}))

	select {
	case result := <-results:
		expectedModel := nb.Aggregate([]nb.LocalCounts{nb.CountLocal(train)})
		expectedMetrics := nb.Evaluate(expectedModel, test)
		assert.Equal(t, expectedMetrics, result.Metrics)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for FL round result")
	}
}
