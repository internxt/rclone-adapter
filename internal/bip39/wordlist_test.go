package bip39

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/internxt/rclone-adapter/internal/bip39/wordlists"
)

// TestEnglishWordListHash checks the English word list against the sha256 of
// the official https://github.com/bitcoin/bips/blob/master/bip-0039/english.txt
// Any change to it would change which mnemonics are accepted.
func TestEnglishWordListHash(t *testing.T) {
	const want = "2f5eed53a4727b4bf8880d8f3f199efc90e58503646d9ff8eff3a2ed3b24dbda"
	sum := sha256.Sum256([]byte(strings.Join(wordlists.English, "\n") + "\n"))
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Errorf("English word list sha256 = %s, want %s", got, want)
	}
}
