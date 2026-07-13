// Package nb implements the federated multinomial Naive Bayes model: local
// word counting, FedAvg aggregation, prediction and evaluation.
package nb

import (
	"math"

	"github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/dataset"
)

// LocalCounts is what a single Worker computes from its local partition and
// ships to the Aggregator: raw word/class frequencies, never raw messages.
type LocalCounts struct {
	WordCounts  map[string]map[string]int // class -> word -> count
	ClassTotals map[string]int            // class -> total token count
}

// CountLocal tokenizes every record's text and tallies token frequency per class.
func CountLocal(records []dataset.Record) LocalCounts {
	wordCounts := make(map[string]map[string]int)
	classTotals := make(map[string]int)

	for _, r := range records {
		if _, ok := wordCounts[r.Label]; !ok {
			wordCounts[r.Label] = make(map[string]int)
		}
		for _, tok := range dataset.Tokenize(r.Text) {
			wordCounts[r.Label][tok]++
			classTotals[r.Label]++
		}
	}
	return LocalCounts{WordCounts: wordCounts, ClassTotals: classTotals}
}

// GlobalModel is the aggregated federated (or centralized) Naive Bayes model.
type GlobalModel struct {
	WordProbs   map[string]map[string]float64 // class -> word -> P(word|class)
	ClassPriors map[string]float64            // class -> P(class)
	VocabSize   int
}

// Aggregate implements the FedAvg step: parameters are additive, so
// aggregation is just summing word/class counts across every worker update,
// then deriving Laplace-smoothed probabilities from the combined totals.
//
//	P(t|c) = (sum_i count_i(t,c) + 1) / (sum_i total_i(c) + |V|)
//
// Class priors are derived from token-frequency share rather than message
// counts, since that is the only class-level statistic the Worker -> Aggregator
// protocol carries (classTotals is a token count, not a message count).
func Aggregate(updates []LocalCounts) GlobalModel {
	combinedWordCounts := make(map[string]map[string]int)
	combinedClassTotals := make(map[string]int)
	vocab := make(map[string]struct{})

	for _, u := range updates {
		for class, words := range u.WordCounts {
			if _, ok := combinedWordCounts[class]; !ok {
				combinedWordCounts[class] = make(map[string]int)
			}
			for word, count := range words {
				combinedWordCounts[class][word] += count
				vocab[word] = struct{}{}
			}
		}
		for class, total := range u.ClassTotals {
			combinedClassTotals[class] += total
		}
	}

	vocabSize := len(vocab)
	totalTokens := 0
	for _, total := range combinedClassTotals {
		totalTokens += total
	}

	wordProbs := make(map[string]map[string]float64)
	classPriors := make(map[string]float64)
	for class, total := range combinedClassTotals {
		probs := make(map[string]float64, vocabSize)
		for word := range vocab {
			count := combinedWordCounts[class][word]
			probs[word] = float64(count+1) / float64(total+vocabSize)
		}
		wordProbs[class] = probs

		if totalTokens > 0 {
			classPriors[class] = float64(total) / float64(totalTokens)
		}
	}

	return GlobalModel{WordProbs: wordProbs, ClassPriors: classPriors, VocabSize: vocabSize}
}

// Predict returns the class with the highest posterior log-probability for
// text. Tokens outside the model's vocabulary carry no information and are
// skipped rather than penalized.
func Predict(model GlobalModel, text string) string {
	best := ""
	bestScore := math.Inf(-1)

	for class, prior := range model.ClassPriors {
		if prior <= 0 {
			continue
		}
		score := math.Log(prior)
		for _, tok := range dataset.Tokenize(text) {
			if p, ok := model.WordProbs[class][tok]; ok {
				score += math.Log(p)
			}
		}
		if score > bestScore {
			bestScore = score
			best = class
		}
	}
	return best
}

// Metrics holds the standard binary-classification evaluation numbers, with
// "spam" treated as the positive class.
type Metrics struct {
	Accuracy  float64
	Precision float64
	Recall    float64
	F1        float64
}

// Evaluate runs model against a labeled test set and computes
// accuracy/precision/recall/F1 with spam as the positive class.
func Evaluate(model GlobalModel, test []dataset.Record) Metrics {
	var tp, fp, tn, fn int
	for _, r := range test {
		predSpam := Predict(model, r.Text) == dataset.LabelSpam
		actualSpam := r.Label == dataset.LabelSpam

		switch {
		case actualSpam && predSpam:
			tp++
		case !actualSpam && predSpam:
			fp++
		case !actualSpam && !predSpam:
			tn++
		default:
			fn++
		}
	}

	var m Metrics
	if total := tp + fp + tn + fn; total > 0 {
		m.Accuracy = float64(tp+tn) / float64(total)
	}
	if tp+fp > 0 {
		m.Precision = float64(tp) / float64(tp+fp)
	}
	if tp+fn > 0 {
		m.Recall = float64(tp) / float64(tp+fn)
	}
	if m.Precision+m.Recall > 0 {
		m.F1 = 2 * m.Precision * m.Recall / (m.Precision + m.Recall)
	}
	return m
}
