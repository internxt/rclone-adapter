package buckets

import (
	"bytes"
	"encoding/hex"
	"io"
	"testing"
)

var (
	TEST_BUCKET_ID = []byte{
		0x01, 0x23, 0x45, 0x67,
		0x89, 0xab, 0xcd, 0xef,
		0x00, 0x00,
	}
)

func TestGenerateBucketKey(t *testing.T) {
	want := "726a02ad035960f8b6563497557bb8efe15cdb160ffb40541102c92c89262a00"
	got, _ := GenerateBucketKey(TestMnemonic, TEST_BUCKET_ID)
	if want != got {
		t.Fatalf("Wanted %s, but got %s", want, got)
	}
}

func TestGetFileDeterministicKey(t *testing.T) {
	want := "a4321694c796a075a91818192f0fe66ccc0ad8a9b75251e8034b6661a7ea97e5e347e5ce0be65a23a8e6eefa205e2d27651de21013589dfb7fde458588f84314"
	got := hex.EncodeToString(GetFileDeterministicKey([]byte(TestMnemonic), []byte(TestMnemonic)))
	if want != got {
		t.Fatalf("Wanted %s, but got %s", want, got)
	}
}

func TestGetDeterministicKey(t *testing.T) {
	want := "8eed4cfe5cb8fa1287356b520bb956085aa1926c825289c7d27e989aa74e7a3c9d18ad1308c5eff69e6ff8dc9059cd84afdd665c462ed6f0d6dbf7540a265ccf"
	got, _ := GetDeterministicKey(TEST_BUCKET_ID, TEST_BUCKET_ID)
	gotString := hex.EncodeToString(got)
	if want != gotString {
		t.Fatalf("Wanted %s, but got %s", want, gotString)
	}
}

func TestCalculateFileHash(t *testing.T) {
	want := "30899ccba67493659474c5397a3e860cd45a670c"
	test := bytes.NewReader(TEST_BUCKET_ID)
	got, _ := CalculateFileHash(test)
	if want != got {
		t.Fatalf("Wanted %s, but got %s", want, got)
	}
}

func TestGenerateFileKey(t *testing.T) {
	wantKey := "d71b781ecf61d8553b0326031658c575c7bec5f92bdeb9ed08925317d2c22e59"
	tempIV, _ := hex.DecodeString(TestIndex)
	wantIV := hex.EncodeToString(tempIV[0:16])
	gotKey, gotIV, _ := GenerateFileKey(TestMnemonic, hex.EncodeToString(TEST_BUCKET_ID), TestIndex)
	gotKeyString := hex.EncodeToString(gotKey)
	gotIVString := hex.EncodeToString(gotIV)

	if wantKey != gotKeyString || wantIV != gotIVString {
		t.Fatalf("\nWanted %s and %s\ngot %s and %s", wantKey, wantIV, gotKeyString, gotIVString)
	}
}

func TestNewAES256CTRCipher(t *testing.T) {
	t.Run("valid key and IV", func(t *testing.T) {
		key := make([]byte, 32) // 32 bytes for AES-256
		iv := make([]byte, 16)  // 16 bytes for IV
		for i := range key {
			key[i] = byte(i)
		}
		for i := range iv {
			iv[i] = byte(i)
		}

		stream, err := NewAES256CTRCipher(key, iv)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if stream == nil {
			t.Fatal("expected stream, got nil")
		}
	})

	t.Run("invalid key length - too short", func(t *testing.T) {
		key := make([]byte, 8) // 8 bytes - too short
		iv := make([]byte, 16)

		_, err := NewAES256CTRCipher(key, iv)
		if err == nil {
			t.Fatal("expected error for invalid key length, got nil")
		}
	})

	t.Run("invalid IV length", func(t *testing.T) {
		key := make([]byte, 32)
		iv := make([]byte, 8) // 8 bytes - too short

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for invalid IV length")
			}
		}()

		_, err := NewAES256CTRCipher(key, iv)
		if err == nil {
			// If no error, the panic should have occurred
		}
	})
}

func TestEncryptReader(t *testing.T) {
	t.Run("successful encryption", func(t *testing.T) {
		key := make([]byte, 32)
		iv := make([]byte, 16)
		for i := range key {
			key[i] = byte(i)
		}
		for i := range iv {
			iv[i] = byte(i)
		}

		testData := []byte("test data to encrypt")
		src := bytes.NewReader(testData)

		encReader, err := EncryptReader(src, key, iv)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		encryptedData, err := io.ReadAll(encReader)
		if err != nil {
			t.Fatalf("failed to read encrypted data: %v", err)
		}

		if len(encryptedData) != len(testData) {
			t.Errorf("expected encrypted data length %d, got %d", len(testData), len(encryptedData))
		}

		if bytes.Equal(encryptedData, testData) {
			t.Error("encrypted data should be different from original")
		}
	})

	t.Run("error - invalid key length", func(t *testing.T) {
		key := make([]byte, 8)
		iv := make([]byte, 16)
		src := bytes.NewReader([]byte("test"))

		_, err := EncryptReader(src, key, iv)
		if err == nil {
			t.Fatal("expected error for invalid key length, got nil")
		}
		if !bytes.Contains([]byte(err.Error()), []byte("failed to create encryption stream")) {
			t.Errorf("expected error about encryption stream, got %v", err)
		}
	})
}

func TestDecryptReader(t *testing.T) {
	t.Run("successful decryption", func(t *testing.T) {
		key := make([]byte, 32)
		iv := make([]byte, 16)
		for i := range key {
			key[i] = byte(i)
		}
		for i := range iv {
			iv[i] = byte(i)
		}

		testData := []byte("test data")
		encStream, _ := NewAES256CTRCipher(key, iv)
		encryptedData := make([]byte, len(testData))
		encStream.XORKeyStream(encryptedData, testData)

		encSrc := bytes.NewReader(encryptedData)

		decReader, err := DecryptReader(encSrc, key, iv)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		decryptedData, err := io.ReadAll(decReader)
		if err != nil {
			t.Fatalf("failed to read decrypted data: %v", err)
		}

		if !bytes.Equal(decryptedData, testData) {
			t.Errorf("expected decrypted data %q, got %q", string(testData), string(decryptedData))
		}
	})

	t.Run("error - invalid key length", func(t *testing.T) {
		key := make([]byte, 8)
		iv := make([]byte, 16)
		src := bytes.NewReader([]byte("test"))

		_, err := DecryptReader(src, key, iv)
		if err == nil {
			t.Fatal("expected error for invalid key length, got nil")
		}
		if !bytes.Contains([]byte(err.Error()), []byte("failed to create AES cipher")) {
			t.Errorf("expected error about AES cipher, got %v", err)
		}
	})
}

func TestGenerateFileBucketKey(t *testing.T) {
	t.Run("successful generation", func(t *testing.T) {
		mnemonic := TestMnemonic
		bucketID := hex.EncodeToString(TEST_BUCKET_ID)

		key, err := GenerateFileBucketKey(mnemonic, bucketID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key == nil {
			t.Fatal("expected key, got nil")
		}
		if len(key) != 64 {
			t.Errorf("expected key length 64, got %d", len(key))
		}
	})

	t.Run("error - invalid hex bucket ID", func(t *testing.T) {
		mnemonic := TestMnemonic
		invalidBucketID := "invalid-hex"

		_, err := GenerateFileBucketKey(mnemonic, invalidBucketID)
		if err == nil {
			t.Fatal("expected error for invalid hex bucket ID, got nil")
		}
		if !bytes.Contains([]byte(err.Error()), []byte("failed to decode bucket ID")) {
			t.Errorf("expected error about decoding bucket ID, got %v", err)
		}
	})

	t.Run("empty bucket ID - should work but produce empty key", func(t *testing.T) {
		mnemonic := TestMnemonic
		emptyBucketID := ""

		key, err := GenerateFileBucketKey(mnemonic, emptyBucketID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if key == nil {
			t.Fatal("expected key, got nil")
		}
	})
}
