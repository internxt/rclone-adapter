package buckets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/endpoints"
)

const (
	testFileUUID = "file-uuid-12345"
	testIndex    = "0123456789abcdef00000123456789abcdef00000123456789abcdef00000000"
)

// TestDownloadFile_ValidHash : Successful download with hash validation
func TestDownloadFile_ValidHash(t *testing.T) {
	plainData := []byte("test file content for hash validation")

	key, iv, err := GenerateFileKey(testMnemonic, testBucketID, testIndex)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	encReader, err := EncryptReader(bytes.NewReader(plainData), key, iv)
	if err != nil {
		t.Fatalf("failed to create encrypt reader: %v", err)
	}

	encData, err := io.ReadAll(encReader)
	if err != nil {
		t.Fatalf("failed to read encrypted data: %v", err)
	}

	sha256Hasher := sha256.New()
	sha256Hasher.Write(encData)
	expectedHash := ComputeFileHash(sha256Hasher.Sum(nil))

	var infoServer, downloadServer *httptest.Server

	downloadServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(encData)
	}))
	defer downloadServer.Close()

	infoServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, testFileUUID) {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		resp := BucketFileInfo{
			Bucket: testBucketID,
			Index:  testIndex,
			Size:   int64(len(plainData)),
			ID:     testFileUUID,
			Shards: []ShardInfo{
				{
					Index: 0,
					Hash:  expectedHash,
					URL:   downloadServer.URL + "/shard",
				},
			},
		}

		json.NewEncoder(w).Encode(resp)
	}))
	defer infoServer.Close()

	cfg := &config.Config{
		Mnemonic:           testMnemonic,
		Bucket:             testBucketID,
		BasicAuthHeader:    "Basic test",
		HTTPClient:         &http.Client{},
		Endpoints:          endpoints.NewConfig(infoServer.URL),
		SkipHashValidation: false, // Enable validation
	}

	tmpFile := t.TempDir() + "/download.dat"
	err = DownloadFile(context.Background(), cfg, testFileUUID, tmpFile)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	downloaded, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(downloaded, plainData) {
		t.Errorf("downloaded content mismatch:\nwant: %s\ngot:  %s", plainData, downloaded)
	}
}

// TestDownloadFile_HashMismatch : Hash mismatch error
func TestDownloadFile_HashMismatch(t *testing.T) {
	plainData := []byte("test content")

	key, iv, _ := GenerateFileKey(testMnemonic, testBucketID, testIndex)
	encReader, _ := EncryptReader(bytes.NewReader(plainData), key, iv)
	encData, _ := io.ReadAll(encReader)

	corruptedData := make([]byte, len(encData))
	copy(corruptedData, encData)
	corruptedData[len(corruptedData)-1] ^= 0xFF // Flip last byte

	sha256Hasher := sha256.New()
	sha256Hasher.Write(encData)
	expectedHash := ComputeFileHash(sha256Hasher.Sum(nil))

	var infoServer, downloadServer *httptest.Server

	downloadServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(corruptedData)
	}))
	defer downloadServer.Close()

	infoServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := BucketFileInfo{
			Bucket: testBucketID,
			Index:  testIndex,
			Size:   int64(len(plainData)),
			ID:     testFileUUID,
			Shards: []ShardInfo{{Index: 0, Hash: expectedHash, URL: downloadServer.URL + "/shard"}},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer infoServer.Close()

	cfg := &config.Config{
		Mnemonic:           testMnemonic,
		Bucket:             testBucketID,
		BasicAuthHeader:    "Basic test",
		HTTPClient:         &http.Client{},
		Endpoints:          endpoints.NewConfig(infoServer.URL),
		SkipHashValidation: false,
	}

	tmpFile := t.TempDir() + "/corrupted.dat"
	err := DownloadFile(context.Background(), cfg, testFileUUID, tmpFile)

	if err == nil {
		t.Fatal("expected hash mismatch error, got nil")
	}

	if !strings.Contains(err.Error(), "hash mismatch") {
		t.Errorf("expected 'hash mismatch' error, got: %v", err)
	}

	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Error("corrupted file should have been removed")
	}
}

// TestDownloadFile_SkipValidation : Skip hash validation flag
func TestDownloadFile_SkipValidation(t *testing.T) {
	plainData := []byte("test content")

	key, iv, _ := GenerateFileKey(testMnemonic, testBucketID, testIndex)
	encReader, _ := EncryptReader(bytes.NewReader(plainData), key, iv)
	corruptedData, _ := io.ReadAll(encReader)

	corruptedData[0] ^= 0xFF

	var infoServer, downloadServer *httptest.Server

	downloadServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(corruptedData)
	}))
	defer downloadServer.Close()

	infoServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := BucketFileInfo{
			Bucket: testBucketID,
			Index:  testIndex,
			Size:   int64(len(plainData)),
			ID:     testFileUUID,
			Shards: []ShardInfo{{Index: 0, Hash: "wrong-hash-intentionally", URL: downloadServer.URL + "/shard"}},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer infoServer.Close()

	cfg := &config.Config{
		Mnemonic:           testMnemonic,
		Bucket:             testBucketID,
		BasicAuthHeader:    "Basic test",
		HTTPClient:         &http.Client{},
		Endpoints:          endpoints.NewConfig(infoServer.URL),
		SkipHashValidation: true, // SKIP validation
	}

	tmpFile := t.TempDir() + "/skip-validation.dat"
	err := DownloadFile(context.Background(), cfg, testFileUUID, tmpFile)

	if err != nil {
		t.Fatalf("expected success with SkipHashValidation=true, got: %v", err)
	}

	if _, err := os.Stat(tmpFile); err != nil {
		t.Errorf("file should exist: %v", err)
	}
}

// TestDownloadFileStream_FullDownload_ValidHash : DownloadFileStream with full download (hash validated)
func TestDownloadFileStream_FullDownload_ValidHash(t *testing.T) {
	plainData := []byte("streaming test content")

	key, iv, _ := GenerateFileKey(testMnemonic, testBucketID, testIndex)
	encReader, _ := EncryptReader(bytes.NewReader(plainData), key, iv)
	encData, _ := io.ReadAll(encReader)

	sha256Hasher := sha256.New()
	sha256Hasher.Write(encData)
	expectedHash := ComputeFileHash(sha256Hasher.Sum(nil))

	var infoServer, downloadServer *httptest.Server

	downloadServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			t.Errorf("unexpected Range header: %s", r.Header.Get("Range"))
		}
		w.WriteHeader(http.StatusOK)
		w.Write(encData)
	}))
	defer downloadServer.Close()

	infoServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := BucketFileInfo{
			Bucket: testBucketID,
			Index:  testIndex,
			Size:   int64(len(plainData)),
			ID:     testFileUUID,
			Shards: []ShardInfo{{Index: 0, Hash: expectedHash, URL: downloadServer.URL + "/shard"}},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer infoServer.Close()

	cfg := &config.Config{
		Mnemonic:           testMnemonic,
		Bucket:             testBucketID,
		BasicAuthHeader:    "Basic test",
		HTTPClient:         &http.Client{},
		Endpoints:          endpoints.NewConfig(infoServer.URL),
		SkipHashValidation: false,
	}

	stream, err := DownloadFileStream(context.Background(), cfg, testFileUUID)
	if err != nil {
		t.Fatalf("DownloadFileStream failed: %v", err)
	}
	defer stream.Close()

	downloaded, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("failed to read stream: %v", err)
	}

	if !bytes.Equal(downloaded, plainData) {
		t.Errorf("content mismatch:\nwant: %s\ngot:  %s", plainData, downloaded)
	}

	if err := stream.Close(); err != nil {
		t.Errorf("Close() failed with valid hash: %v", err)
	}
}

// TestDownloadFileStream_RangeRequest_NoValidation : DownloadFileStream with range request (no validation)
func TestDownloadFileStream_RangeRequest_NoValidation(t *testing.T) {
	plainData := []byte("0123456789abcdefghijklmnopqrstuvwxyz") // 36 bytes

	key, iv, _ := GenerateFileKey(testMnemonic, testBucketID, testIndex)
	encReader, _ := EncryptReader(bytes.NewReader(plainData), key, iv)
	encData, _ := io.ReadAll(encReader)

	var infoServer, downloadServer *httptest.Server

	downloadServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			t.Error("expected Range header for range request")
		}

		w.Header().Set("Content-Range", "bytes 0-15/36")
		w.WriteHeader(http.StatusPartialContent)
		w.Write(encData[:16])
	}))
	defer downloadServer.Close()

	infoServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := BucketFileInfo{
			Bucket: testBucketID,
			Index:  testIndex,
			Size:   int64(len(plainData)),
			ID:     testFileUUID,
			Shards: []ShardInfo{{Index: 0, Hash: "wrong-hash-should-be-ignored", URL: downloadServer.URL + "/shard"}},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer infoServer.Close()

	cfg := &config.Config{
		Mnemonic:           testMnemonic,
		Bucket:             testBucketID,
		BasicAuthHeader:    "Basic test",
		HTTPClient:         &http.Client{},
		Endpoints:          endpoints.NewConfig(infoServer.URL),
		SkipHashValidation: false,
	}

	stream, err := DownloadFileStream(context.Background(), cfg, testFileUUID, "bytes=0-15")
	if err != nil {
		t.Fatalf("DownloadFileStream failed: %v", err)
	}
	defer stream.Close()

	_, err = io.ReadAll(stream)
	if err != nil {
		t.Fatalf("failed to read stream: %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Errorf("Close() should not fail for range request: %v", err)
	}
}

// TestDownloadFile_HTTPErrors : HTTP error codes
func TestDownloadFile_HTTPErrors(t *testing.T) {
	testCases := []struct {
		name          string
		statusCode    int
		errorContains string
	}{
		{
			name:          "404 not found",
			statusCode:    http.StatusNotFound,
			errorContains: "404",
		},
		{
			name:          "401 unauthorized",
			statusCode:    http.StatusUnauthorized,
			errorContains: "401",
		},
		{
			name:          "500 server error",
			statusCode:    http.StatusInternalServerError,
			errorContains: "500",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var infoServer, downloadServer *httptest.Server

			downloadServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
				w.Write([]byte("error response"))
			}))
			defer downloadServer.Close()

			infoServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				resp := BucketFileInfo{
					Bucket: testBucketID,
					Index:  testIndex,
					Size:   100,
					ID:     testFileUUID,
					Shards: []ShardInfo{{Index: 0, Hash: "hash", URL: downloadServer.URL + "/shard"}},
				}
				json.NewEncoder(w).Encode(resp)
			}))
			defer infoServer.Close()

			cfg := &config.Config{
				Mnemonic:        testMnemonic,
				Bucket:          testBucketID,
				BasicAuthHeader: "Basic test",
				HTTPClient:      &http.Client{},
				Endpoints:       endpoints.NewConfig(infoServer.URL),
			}

			err := DownloadFile(context.Background(), cfg, testFileUUID, t.TempDir()+"/test.dat")

			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if !strings.Contains(err.Error(), tc.errorContains) {
				t.Errorf("expected error containing %q, got: %v", tc.errorContains, err)
			}
		})
	}
}

// TestDownloadFileStream_HashMismatch : DownloadFileStream hash mismatch
func TestDownloadFileStream_HashMismatch(t *testing.T) {
	plainData := []byte("test stream content")

	key, iv, _ := GenerateFileKey(testMnemonic, testBucketID, testIndex)
	encReader, _ := EncryptReader(bytes.NewReader(plainData), key, iv)
	encData, _ := io.ReadAll(encReader)

	corruptedData := make([]byte, len(encData))
	copy(corruptedData, encData)
	corruptedData[0] ^= 0xFF

	sha256Hasher := sha256.New()
	sha256Hasher.Write(encData)
	expectedHash := ComputeFileHash(sha256Hasher.Sum(nil))

	var infoServer, downloadServer *httptest.Server

	downloadServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(corruptedData)
	}))
	defer downloadServer.Close()

	infoServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := BucketFileInfo{
			Bucket: testBucketID,
			Index:  testIndex,
			Size:   int64(len(plainData)),
			ID:     testFileUUID,
			Shards: []ShardInfo{{Index: 0, Hash: expectedHash, URL: downloadServer.URL + "/shard"}},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer infoServer.Close()

	cfg := &config.Config{
		Mnemonic:           testMnemonic,
		Bucket:             testBucketID,
		BasicAuthHeader:    "Basic test",
		HTTPClient:         &http.Client{},
		Endpoints:          endpoints.NewConfig(infoServer.URL),
		SkipHashValidation: false,
	}

	stream, err := DownloadFileStream(context.Background(), cfg, testFileUUID)
	if err != nil {
		t.Fatalf("DownloadFileStream failed: %v", err)
	}

	_, err = io.ReadAll(stream)
	if err != nil {
		t.Fatalf("failed to read stream: %v", err)
	}

	err = stream.Close()
	if err == nil {
		t.Fatal("expected hash mismatch error on Close(), got nil")
	}

	if !strings.Contains(err.Error(), "hash mismatch") {
		t.Errorf("expected 'hash mismatch' error, got: %v", err)
	}
}

// TestDownloadFile_EmptyFile : Empty file download
func TestDownloadFile_EmptyFile(t *testing.T) {
	plainData := []byte("")

	key, iv, _ := GenerateFileKey(testMnemonic, testBucketID, testIndex)
	encReader, _ := EncryptReader(bytes.NewReader(plainData), key, iv)
	encData, _ := io.ReadAll(encReader)

	sha256Hasher := sha256.New()
	sha256Hasher.Write(encData)
	expectedHash := ComputeFileHash(sha256Hasher.Sum(nil))

	var infoServer, downloadServer *httptest.Server

	downloadServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(encData)
	}))
	defer downloadServer.Close()

	infoServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := BucketFileInfo{
			Bucket: testBucketID,
			Index:  testIndex,
			Size:   0,
			ID:     testFileUUID,
			Shards: []ShardInfo{{Index: 0, Hash: expectedHash, URL: downloadServer.URL + "/shard"}},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer infoServer.Close()

	cfg := &config.Config{
		Mnemonic:           testMnemonic,
		Bucket:             testBucketID,
		BasicAuthHeader:    "Basic test",
		HTTPClient:         &http.Client{},
		Endpoints:          endpoints.NewConfig(infoServer.URL),
		SkipHashValidation: false,
	}

	tmpFile := t.TempDir() + "/empty.dat"
	err := DownloadFile(context.Background(), cfg, testFileUUID, tmpFile)
	if err != nil {
		t.Fatalf("DownloadFile failed for empty file: %v", err)
	}

	downloaded, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if len(downloaded) != 0 {
		t.Errorf("expected empty file, got %d bytes", len(downloaded))
	}
}
