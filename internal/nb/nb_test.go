package nb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/dataset"
)

func TestCountLocal(t *testing.T) {
	records := []dataset.Record{
		{Label: dataset.LabelHam, Text: "hello world"},
		{Label: dataset.LabelSpam, Text: "win money now"},
		{Label: dataset.LabelSpam, Text: "win a prize"},
	}

	counts := CountLocal(records)
	assert.Equal(t, 1, counts.WordCounts[dataset.LabelHam]["hello"])
	assert.Equal(t, 1, counts.WordCounts[dataset.LabelHam]["world"])
	assert.Equal(t, 2, counts.WordCounts[dataset.LabelSpam]["win"])
	assert.Equal(t, 2, counts.ClassTotals[dataset.LabelHam])
	assert.Equal(t, 6, counts.ClassTotals[dataset.LabelSpam])
}

func TestAggregateAppliesLaplaceSmoothing(t *testing.T) {
	// Two workers, each contributing disjoint counts for a two-word vocabulary.
	updates := []LocalCounts{
		{
			WordCounts:  map[string]map[string]int{"ham": {"hello": 3}},
			ClassTotals: map[string]int{"ham": 3},
		},
		{
			WordCounts:  map[string]map[string]int{"spam": {"win": 5}},
			ClassTotals: map[string]int{"spam": 5},
		},
	}

	model := Aggregate(updates)
	require.Equal(t, 2, model.VocabSize) // {"hello", "win"}

	// P(hello|ham) = (3+1)/(3+2) = 0.8 ; P(win|ham) = (0+1)/(3+2) = 0.2
	assert.InDelta(t, 0.8, model.WordProbs["ham"]["hello"], 1e-9)
	assert.InDelta(t, 0.2, model.WordProbs["ham"]["win"], 1e-9)

	// P(win|spam) = (5+1)/(5+2) = 6/7 ; P(hello|spam) = (0+1)/(5+2) = 1/7
	assert.InDelta(t, 6.0/7.0, model.WordProbs["spam"]["win"], 1e-9)
	assert.InDelta(t, 1.0/7.0, model.WordProbs["spam"]["hello"], 1e-9)

	// Priors follow token-frequency share: ham=3/8, spam=5/8.
	assert.InDelta(t, 3.0/8.0, model.ClassPriors["ham"], 1e-9)
	assert.InDelta(t, 5.0/8.0, model.ClassPriors["spam"], 1e-9)
}

func TestAggregateIsOrderIndependentAndAdditive(t *testing.T) {
	records := []dataset.Record{
		{Label: dataset.LabelHam, Text: "hello there friend"},
		{Label: dataset.LabelHam, Text: "how are you today"},
		{Label: dataset.LabelSpam, Text: "win free money now"},
		{Label: dataset.LabelSpam, Text: "claim your prize now"},
	}

	// Aggregating the whole dataset as one shard...
	whole := Aggregate([]LocalCounts{CountLocal(records)})

	// ...must equal aggregating it split across two shards.
	shard1 := CountLocal(records[:2])
	shard2 := CountLocal(records[2:])
	split := Aggregate([]LocalCounts{shard1, shard2})

	assert.Equal(t, whole.WordProbs, split.WordProbs)
	assert.Equal(t, whole.ClassPriors, split.ClassPriors)
}

func TestPredictAndEvaluate(t *testing.T) {
	train := []dataset.Record{
		{Label: dataset.LabelHam, Text: "hello friend how are you"},
		{Label: dataset.LabelHam, Text: "are we meeting today"},
		{Label: dataset.LabelSpam, Text: "win free money now"},
		{Label: dataset.LabelSpam, Text: "claim your free prize now"},
	}
	model := Aggregate([]LocalCounts{CountLocal(train)})

	assert.Equal(t, dataset.LabelHam, Predict(model, "are you free today"))
	assert.Equal(t, dataset.LabelSpam, Predict(model, "win your free prize now"))

	test := []dataset.Record{
		{Label: dataset.LabelHam, Text: "hello friend"},
		{Label: dataset.LabelSpam, Text: "win free prize now"},
	}
	metrics := Evaluate(model, test)
	assert.Equal(t, 1.0, metrics.Accuracy)
	assert.Equal(t, 1.0, metrics.Precision)
	assert.Equal(t, 1.0, metrics.Recall)
	assert.Equal(t, 1.0, metrics.F1)
}
