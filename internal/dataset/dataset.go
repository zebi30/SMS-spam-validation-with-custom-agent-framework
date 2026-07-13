// Package dataset loads and prepares the SMS Spam Collection dataset for
// both centralized and federated training.
package dataset

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
)

// Class labels used throughout the dataset and the Naive Bayes model.
const (
	LabelSpam = "spam"
	LabelHam  = "ham"
)

// Record is a single labeled SMS message.
type Record struct {
	Label string
	Text  string
}

var tokenPattern = regexp.MustCompile(`[a-zA-Z0-9]+`)

// Tokenize lowercases text and splits it into alphanumeric word tokens.
func Tokenize(text string) []string {
	return tokenPattern.FindAllString(strings.ToLower(text), -1)
}

// Load reads a TSV file in "label\ttext" format, one message per line.
func Load(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("dataset: open %s: %w", path, err)
	}
	defer f.Close()

	var records []Record
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("dataset: malformed line %q in %s", line, path)
		}
		records = append(records, Record{Label: parts[0], Text: parts[1]})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("dataset: read %s: %w", path, err)
	}
	return records, nil
}

// Save writes records back out in the same "label\ttext" TSV format Load expects.
func Save(records []Record, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("dataset: create %s: %w", path, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, r := range records {
		if _, err := fmt.Fprintf(w, "%s\t%s\n", r.Label, r.Text); err != nil {
			return fmt.Errorf("dataset: write %s: %w", path, err)
		}
	}
	return w.Flush()
}

// SplitTrainTest shuffles records deterministically (given seed) and splits
// them into a training set of trainRatio and a remaining test set.
func SplitTrainTest(records []Record, trainRatio float64, seed int64) (train, test []Record) {
	shuffled := make([]Record, len(records))
	copy(shuffled, records)

	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	splitAt := int(float64(len(shuffled)) * trainRatio)
	train = shuffled[:splitAt]
	test = shuffled[splitAt:]
	return train, test
}

// Partition splits records into n roughly-equal, contiguous shards, one per
// Worker node.
func Partition(records []Record, n int) [][]Record {
	if n <= 0 {
		return nil
	}
	shards := make([][]Record, n)
	base := len(records) / n
	remainder := len(records) % n

	start := 0
	for i := 0; i < n; i++ {
		size := base
		if i < remainder {
			size++
		}
		shards[i] = records[start : start+size]
		start += size
	}
	return shards
}
