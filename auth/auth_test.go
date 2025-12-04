package auth

import (
	"strings"
	"testing"
)

func TestDecryptTextWithKey(t *testing.T) {
	want := "this was encrypted"
	got, err := decryptTextWithKey("53616c7465645f5f78753b7d2417893c685726f8c5d5778e2e591e528f657270d3f94ff26ccc440112d94eea8308ce10", "secret")
	if err != nil {
		t.Fatalf("decryptTextWithKey failed: %v", err)
	}
	if want != got {
		t.Fatalf("wanted %q, but got %q", want, got)
	}
}

func TestEncryptTextWithKey(t *testing.T) {
	want := "this was encrypted"
	gotEncrypted, err := encryptTextWithKey("this was encrypted", "secret")
	if err != nil {
		t.Fatalf("encryptTextWithKey failed: %v", err)
	}
	gotDecrypted, err := decryptTextWithKey(gotEncrypted, "secret")
	if err != nil {
		t.Fatalf("decryptTextWithKey failed: %v", err)
	}
	if want != gotDecrypted {
		t.Fatalf("wanted %q, but got %q", want, gotDecrypted)
	}
}

// TestEncryptTextWithKey_NonDeterministic verifies that encryption produces different
// ciphertexts for the same plaintext (due to random salt)
func TestEncryptTextWithKey_NonDeterministic(t *testing.T) {
	plaintext := "test message"
	secret := "my-secret-key"

	// Encrypt the same message 10 times
	ciphertexts := make([]string, 10)
	for i := range 10 {
		ct, err := encryptTextWithKey(plaintext, secret)
		if err != nil {
			t.Fatalf("encryption %d failed: %v", i, err)
		}
		ciphertexts[i] = ct
	}

	// Verify all ciphertexts are different (random salt)
	for i := range ciphertexts {
		for j := range ciphertexts {
			if i != j && ciphertexts[i] == ciphertexts[j] {
				t.Errorf("encryption is deterministic: ciphertext %d equals ciphertext %d", i, j)
			}
		}
	}

	// Verify all decrypt to the same plaintext
	for i, ct := range ciphertexts {
		pt, err := decryptTextWithKey(ct, secret)
		if err != nil {
			t.Errorf("decryption %d failed: %v", i, err)
		}
		if pt != plaintext {
			t.Errorf("decryption %d: expected %q, got %q", i, plaintext, pt)
		}
	}
}

// TestDecryptTextWithKey_InvalidPadding tests that invalid padding is rejected
func TestDecryptTextWithKey_InvalidPadding(t *testing.T) {
	tests := []struct {
		name       string
		hexCipher  string
		wantErrMsg string
	}{
		{
			name:       "too short",
			hexCipher:  "53616c7465645f5f",
			wantErrMsg: "too short",
		},
		{
			name:       "missing Salted__ header",
			hexCipher:  "0000000078753b7d2417893c685726f8c5d5778e2e591e528f657270d3f94ff26ccc440112d94eea8308ce10",
			wantErrMsg: "missing Salted__ header",
		},
		{
			name:       "invalid hex",
			hexCipher:  "ZZZZZZ",
			wantErrMsg: "invalid hex",
		},
		{
			name:       "not block size multiple",
			hexCipher:  "53616c7465645f5f78753b7d2417893c685726f8c5d5778e2e591e528f657270d3",
			wantErrMsg: "not a multiple of block size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decryptTextWithKey(tt.hexCipher, "secret")
			if err == nil {
				t.Fatal("expected error but got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Errorf("expected error containing %q, got %q", tt.wantErrMsg, err.Error())
			}
		})
	}
}

// TestEncryptDecrypt_VariousInputSizes tests encryption/decryption with various sizes
func TestEncryptDecrypt_VariousInputSizes(t *testing.T) {
	secret := "test-secret"

	testCases := []struct {
		name      string
		plaintext string
	}{
		{"empty", ""},
		{"single char", "a"},
		{"block size - 1", strings.Repeat("x", 15)},
		{"exact block size", strings.Repeat("x", 16)},
		{"block size + 1", strings.Repeat("x", 17)},
		{"large text", strings.Repeat("Hello World! ", 100)},
		{"unicode", "Hello 世界 🌍"},
		{"special chars", "!@#$%^&*()_+-=[]{}|;':\",./<>?"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encrypted, err := encryptTextWithKey(tc.plaintext, secret)
			if err != nil {
				t.Fatalf("encryption failed: %v", err)
			}

			decrypted, err := decryptTextWithKey(encrypted, secret)
			if err != nil {
				t.Fatalf("decryption failed: %v", err)
			}

			if decrypted != tc.plaintext {
				t.Errorf("mismatch: expected %q, got %q", tc.plaintext, decrypted)
			}
		})
	}
}

// TestDecryptTextWithKey_WrongSecret verifies that wrong secret produces error
func TestDecryptTextWithKey_WrongSecret(t *testing.T) {
	plaintext := "secret message"
	correctSecret := "correct-secret"
	wrongSecret := "wrong-secret"

	encrypted, err := encryptTextWithKey(plaintext, correctSecret)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	decrypted, err := decryptTextWithKey(encrypted, wrongSecret)

	if err == nil && decrypted == plaintext {
		t.Error("decryption with wrong secret should not succeed")
	}
}
