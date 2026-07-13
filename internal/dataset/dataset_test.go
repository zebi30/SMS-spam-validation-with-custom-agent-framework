package dataset

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenize(t *testing.T) {
	tokens := Tokenize("Free entry! WIN a prize NOW: call 123-456.")
	assert.Equal(t, []string{"free", "entry", "win", "a", "prize", "now", "call", "123", "456"}, tokens)
}

func TestSaveAndLoadRoundTrip(t *testing.T) {
	records := []Record{
		{Label: LabelHam, Text: "hey how are you"},
		{Label: LabelSpam, Text: "win a free prize now"},
	}

	path := filepath.Join(t.TempDir(), "roundtrip.tsv")
	require.NoError(t, Save(records, path))

	loaded, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, records, loaded)
}

func TestLoadRejectsMalformedLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.tsv")
	require.NoError(t, Save(nil, path))

	// Append a line with no tab separator.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteString("this-line-has-no-label-separator\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = Load(path)
	assert.Error(t, err)
}

func TestSplitTrainTest(t *testing.T) {
	records := make([]Record, 100)
	for i := range records {
		records[i] = Record{Label: LabelHam, Text: "message"}
	}

	train, test := SplitTrainTest(records, 0.8, 42)
	assert.Len(t, train, 80)
	assert.Len(t, test, 20)

	// Same seed must reproduce the same split.
	train2, test2 := SplitTrainTest(records, 0.8, 42)
	assert.Equal(t, train, train2)
	assert.Equal(t, test, test2)
}

func TestPartitionPreservesAllRecordsEvenly(t *testing.T) {
	records := make([]Record, 10)
	for i := range records {
		records[i] = Record{Label: LabelHam, Text: "message"}
	}

	shards := Partition(records, 3)
	require.Len(t, shards, 3)

	total := 0
	for _, shard := range shards {
		total += len(shard)
		assert.GreaterOrEqual(t, len(shard), 3)
		assert.LessOrEqual(t, len(shard), 4)
	}
	assert.Equal(t, 10, total)
}
