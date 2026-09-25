package bip39

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestWordList guards against accidental changes to the embedded word list,
// which would change which mnemonics are accepted.
func TestWordList(t *testing.T) {
	const want = "2f5eed53a4727b4bf8880d8f3f199efc90e58503646d9ff8eff3a2ed3b24dbda"
	sum := sha256.Sum256([]byte(english))
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Errorf("english.txt sha256 = %s, want %s (official BIP-39 list)", got, want)
	}
	if len(wordIndex) != 2048 {
		t.Errorf("word list has %d unique words, want 2048", len(wordIndex))
	}
}

// Vectors from https://github.com/trezor/python-mnemonic/blob/master/vectors.json
func TestVectors(t *testing.T) {
	vectors := []struct{ mnemonic, seed string }{
		{
			"abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
			"c55257c360c07c72029aebc1b53c05ed0362ada38ead3e3e9efa3708e53495531f09a6987599d18264c1e1c92f2cf141630c7a3c4ab7c81b2f001698e7463b04",
		},
		{
			"zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo zoo wrong",
			"ac27495480225222079d7be181583751e86f571027b0497b5b5d11218e0a8a13332572917f0f8e5a589620c6f15b11c61dee327651a14c34e18231052e48c069",
		},
		{
			"void come effort suffer camp survey warrior heavy shoot primary clutch crush open amazing screen patrol group space point ten exist slush involve unfold",
			"01f5bced59dec48e362f2c45b5de68b9fd6c92c6634f44d6d40aab69056506f0e35524a518034ddc1192e1dacd32c1ed3eaa3c3b131c88ed8e7e54c49a5d0998",
		},
	}
	for _, v := range vectors {
		if !IsMnemonicValid(v.mnemonic) {
			t.Errorf("IsMnemonicValid(%q) = false, want true", v.mnemonic)
		}
		seed, err := NewSeed(v.mnemonic, "TREZOR")
		if err != nil {
			t.Fatalf("NewSeed(%q): %v", v.mnemonic, err)
		}
		if got := hex.EncodeToString(seed); got != v.seed {
			t.Errorf("NewSeed(%q) = %s, want %s", v.mnemonic, got, v.seed)
		}
	}
}

func TestIsMnemonicValid(t *testing.T) {
	tests := []struct {
		name     string
		mnemonic string
		want     bool
	}{
		{"extra whitespace", " abandon  abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon\tabout\n", true},
		{"bad checksum", "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon", false},
		{"unknown word", "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon foo", false},
		{"uppercase", "Abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about", false},
		{"too few words", "abandon abandon abandon abandon abandon abandon abandon abandon about", false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		if got := IsMnemonicValid(tt.mnemonic); got != tt.want {
			t.Errorf("%s: IsMnemonicValid(%q) = %v, want %v", tt.name, tt.mnemonic, got, tt.want)
		}
	}
}
