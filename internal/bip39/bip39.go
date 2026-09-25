// Package bip39 implements the parts of BIP-39 needed to turn a user's
// mnemonic into encryption keys: mnemonic validation and seed derivation.
//
// It replaces github.com/tyler-smith/go-bip39, whose repository was deleted,
// and keeps its behavior: words are split on any whitespace, must be in the
// English word list (case-sensitive) and must carry a valid checksum, and the
// seed is derived from the mnemonic bytes exactly as given, without NFKD
// normalization.
//
// Specification: https://github.com/bitcoin/bips/blob/master/bip-0039.mediawiki
package bip39

import (
	"crypto/pbkdf2"
	"crypto/sha256"
	"crypto/sha512"
	_ "embed"
	"math/big"
	"strings"
)

// english is the official BIP-39 English word list, copied unmodified from
// https://github.com/bitcoin/bips/blob/master/bip-0039/english.txt
//
//go:embed english.txt
var english string

var wordIndex = func() map[string]int {
	words := strings.Fields(english)
	index := make(map[string]int, len(words))
	for i, w := range words {
		index[w] = i
	}
	return index
}()

// IsMnemonicValid reports whether mnemonic has 12, 15, 18, 21 or 24 words from
// the English word list and a valid checksum.
func IsMnemonicValid(mnemonic string) bool {
	words := strings.Fields(mnemonic)
	n := len(words)
	if n%3 != 0 || n < 12 || n > 24 {
		return false
	}

	b := new(big.Int)
	for _, w := range words {
		i, ok := wordIndex[w]
		if !ok {
			return false
		}
		b.Lsh(b, 11).Or(b, big.NewInt(int64(i)))
	}
	checksumBits := uint(n / 3)
	checksum := new(big.Int).And(b, big.NewInt(1<<checksumBits-1)).Uint64()
	entropy := b.Rsh(b, checksumBits).FillBytes(make([]byte, n/3*4))

	hash := sha256.Sum256(entropy)
	return uint64(hash[0]>>(8-checksumBits)) == checksum
}

// NewSeed derives the 64-byte BIP-39 seed from mnemonic and password. It does
// not validate the mnemonic; call IsMnemonicValid first.
func NewSeed(mnemonic, password string) ([]byte, error) {
	return pbkdf2.Key(sha512.New, mnemonic, []byte("mnemonic"+password), 2048, 64)
}
