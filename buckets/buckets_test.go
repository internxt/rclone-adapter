package buckets

import (
	"bytes"
	"encoding/hex"
	"testing"
)

const (
	TEST_INDEX = "0123456789abcdef00000123456789abcdef00000123456789abcdef00000000"
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
	tempIV, _ := hex.DecodeString(TEST_INDEX)
	wantIV := hex.EncodeToString(tempIV[0:16])
	gotKey, gotIV, _ := GenerateFileKey(TestMnemonic, hex.EncodeToString(TEST_BUCKET_ID), TEST_INDEX)
	gotKeyString := hex.EncodeToString(gotKey)
	gotIVString := hex.EncodeToString(gotIV)

	if wantKey != gotKeyString || wantIV != gotIVString {
		t.Fatalf("\nWanted %s and %s\ngot %s and %s", wantKey, wantIV, gotKeyString, gotIVString)
	}
}
