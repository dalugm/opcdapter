package client

import "testing"

func TestQualityClassification(t *testing.T) {
	for _, test := range []struct {
		quality uint32
		good bool
		description string
	}{
		{0x00, false, "Bad (non-specific)"},
		{0x08, false, "Bad (not connected)"},
		{0x40, false, "Uncertain"},
		{0x44, false, "Uncertain (last usable value)"},
		{0xc0, true, "Good"},
		{0xd8, true, "Good (local override)"},
		{0x12c0, true, "Good"},
	} {
		if got := qualityIsGood(test.quality); got != test.good {
			t.Errorf("qualityIsGood(0x%x) = %v, want %v", test.quality, got, test.good)
		}
		if got := qualityDescription(test.quality); got != test.description {
			t.Errorf("qualityDescription(0x%x) = %q, want %q", test.quality, got, test.description)
		}
	}
}
