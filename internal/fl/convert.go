package fl

import "github.com/zebi30/sms-spam-validation-with-custom-agent-framework/internal/fl/flpb"

// toPBWordCounts wraps a class->word->count map into the nested-message
// shape protobuf requires, since proto3 does not allow a map value to
// itself be a map.
func toPBWordCounts(m map[string]map[string]int) map[string]*flpb.WordCounts {
	out := make(map[string]*flpb.WordCounts, len(m))
	for class, words := range m {
		out[class] = &flpb.WordCounts{Counts: toPBInt64Map(words)}
	}
	return out
}

func fromPBWordCounts(m map[string]*flpb.WordCounts) map[string]map[string]int {
	out := make(map[string]map[string]int, len(m))
	for class, words := range m {
		out[class] = fromPBInt64Map(words.GetCounts())
	}
	return out
}

func toPBWordProbs(m map[string]map[string]float64) map[string]*flpb.WordProbs {
	out := make(map[string]*flpb.WordProbs, len(m))
	for class, probs := range m {
		out[class] = &flpb.WordProbs{Probs: probs}
	}
	return out
}

func fromPBWordProbs(m map[string]*flpb.WordProbs) map[string]map[string]float64 {
	out := make(map[string]map[string]float64, len(m))
	for class, probs := range m {
		out[class] = probs.GetProbs()
	}
	return out
}

func toPBInt64Map(m map[string]int) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = int64(v)
	}
	return out
}

func fromPBInt64Map(m map[string]int64) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = int(v)
	}
	return out
}
