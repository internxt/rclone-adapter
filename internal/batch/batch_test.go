package batch

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

type request struct {
	Names []string `json:"plainNames"`
}

func names(n, length int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = strings.Repeat("文", length)
	}
	return out
}

func body(items []string) any { return request{Names: items} }

func TestSplit(t *testing.T) {
	for _, test := range []struct {
		what        string
		items       []string
		maxItems    int
		maxBytes    int
		wantBatches []int
	}{
		{"no items", nil, 200, MaxBodyBytes, nil},
		{"one batch", names(3, 5), 200, MaxBodyBytes, []int{3}},
		{"exactly max items", names(200, 5), 200, MaxBodyBytes, []int{200}},
		{"split by count", names(450, 5), 200, MaxBodyBytes, []int{200, 200, 50}},
		// each name encodes to 767 bytes, so 133 fit in 102400 bytes
		{"split by bytes", names(300, 255), 200, MaxBodyBytes, []int{133, 133, 34}},
	} {
		t.Run(test.what, func(t *testing.T) {
			batches, err := Split(test.items, test.maxItems, test.maxBytes, body)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var got []int
			for _, batch := range batches {
				got = append(got, len(batch))
				b, _ := json.Marshal(body(batch))
				if len(b) > test.maxBytes {
					t.Errorf("batch of %d items is %d bytes", len(batch), len(b))
				}
			}
			if !slices.Equal(got, test.wantBatches) {
				t.Errorf("got batches %v, want %v", got, test.wantBatches)
			}
		})
	}
}

func TestSplitFillsBytesExactly(t *testing.T) {
	// {"plainNames":["aaa","aaa"]} is exactly 28 bytes
	items := []string{"aaa", "aaa", "aaa"}
	batches, err := Split(items, 200, 28, body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 1 {
		t.Errorf("unexpected batches %v", batches)
	}
}

func TestSplitItemTooLarge(t *testing.T) {
	_, err := Split(names(1, 100), 200, 100, body)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
