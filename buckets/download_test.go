package buckets

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/internxt/rclone-adapter/config"
	"github.com/internxt/rclone-adapter/endpoints"
)

func TestGetBucketFileInfo(t *testing.T) {
	t.Run("successful retrieval", func(t *testing.T) {
		mockResponse := BucketFileInfo{
			Bucket:  TestBucket1,
			Index:   TestIndex,
			Size:    1024,
			Version: 1,
			ID:      TestFileID,
			Shards: []ShardInfo{
				{Index: 0, Hash: "hash1", URL: "https://s3.example.com/shard1"},
			},
		}

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected GET request, got %s", r.Method)
			}

			if r.Header.Get("Authorization") == "" {
				t.Error("Authorization header missing")
			}
			if r.Header.Get("internxt-version") != "1.0" {
				t.Errorf("expected internxt-version 1.0, got %s", r.Header.Get("internxt-version"))
			}
			if r.Header.Get("internxt-client") != "internxt-go-sdk" {
				t.Errorf("expected internxt-client internxt-go-sdk, got %s", r.Header.Get("internxt-client"))
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(mockResponse)
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(mockServer.URL),
		}

		info, err := GetBucketFileInfo(context.Background(), cfg, TestBucket1, TestFileID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if info.ID != mockResponse.ID {
			t.Errorf("expected ID %s, got %s", mockResponse.ID, info.ID)
		}
		if len(info.Shards) != 1 {
			t.Errorf("expected 1 shard, got %d", len(info.Shards))
		}
	})

	t.Run("error - 404 not found", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(mockServer.URL),
		}

		_, err := GetBucketFileInfo(context.Background(), cfg, TestBucket1, "non-existent")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("expected error to contain 404, got %v", err)
		}
	})

	t.Run("error - invalid JSON", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("invalid json"))
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(mockServer.URL),
		}

		_, err := GetBucketFileInfo(context.Background(), cfg, TestBucket1, "file-id")
		if err == nil {
			t.Fatal("expected error for invalid JSON, got nil")
		}
		if !strings.Contains(err.Error(), "failed to decode") {
			t.Errorf("expected error to contain 'failed to decode', got %v", err)
		}
	})
}

func TestDownloadFile(t *testing.T) {
	t.Run("successful download", func(t *testing.T) {
		tmpDir := t.TempDir()
		destPath := filepath.Join(tmpDir, "downloaded-file")

		testData := []byte("test file content")
		plainIndex := TestIndex
		key, iv, err := GenerateFileKey(TestMnemonic, TestBucket6, plainIndex)
		if err != nil {
			t.Fatalf("failed to generate key: %v", err)
		}

		block, _ := aes.NewCipher(key)
		stream := cipher.NewCTR(block, iv)
		encryptedData := make([]byte, len(testData))
		stream.XORKeyStream(encryptedData, testData)

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: ""},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write(encryptedData)
		}))
		defer shardServer.Close()

		infoServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		err = DownloadFile(context.Background(), cfg, TestFileID, destPath)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		downloadedData, err := os.ReadFile(destPath)
		if err != nil {
			t.Fatalf("failed to read downloaded file: %v", err)
		}

		if !bytes.Equal(downloadedData, testData) {
			t.Errorf("expected file content %q, got %q", string(testData), string(downloadedData))
		}
	})

	t.Run("error - no shards", func(t *testing.T) {
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index:  TestIndex,
				Shards: []ShardInfo{},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(mockServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		tmpDir := t.TempDir()
		destPath := filepath.Join(tmpDir, "downloaded-file")

		err := DownloadFile(context.Background(), cfg, TestFileID, destPath)
		if err == nil {
			t.Fatal("expected error for no shards, got nil")
		}
		if !strings.Contains(err.Error(), "no shards found") {
			t.Errorf("expected error to contain 'no shards found', got %v", err)
		}
	})

	t.Run("error - shard download failure", func(t *testing.T) {
		plainIndex := TestIndex

		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/info") {
				info := BucketFileInfo{
					Index: plainIndex,
					Shards: []ShardInfo{
						{Index: 0, Hash: "hash1", URL: "http://invalid-url-that-will-fail"},
					},
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(info)
			}
		}))
		defer mockServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(mockServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		tmpDir := t.TempDir()
		destPath := filepath.Join(tmpDir, "downloaded-file")

		err := DownloadFile(context.Background(), cfg, TestFileID, destPath)
		if err == nil {
			t.Fatal("expected error for shard download failure, got nil")
		}
	})

	t.Run("error - shard returns non-2xx status", func(t *testing.T) {
		plainIndex := TestIndex

		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("shard not found"))
		}))
		defer shardServer.Close()

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		tmpDir := t.TempDir()
		destPath := filepath.Join(tmpDir, "downloaded-file")

		err := DownloadFile(context.Background(), cfg, TestFileID, destPath)
		if err == nil {
			t.Fatal("expected error for non-2xx status, got nil")
		}
		if !strings.Contains(err.Error(), "shard download failed") {
			t.Errorf("expected error to contain 'shard download failed', got %v", err)
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("expected error to contain '404', got %v", err)
		}
	})

	t.Run("error - invalid destination path", func(t *testing.T) {
		plainIndex := TestIndex
		testData := []byte("test file content")
		key, iv, _ := GenerateFileKey(TestMnemonic, TestBucket6, plainIndex)

		block, _ := aes.NewCipher(key)
		stream := cipher.NewCTR(block, iv)
		encryptedData := make([]byte, len(testData))
		stream.XORKeyStream(encryptedData, testData)

		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write(encryptedData)
		}))
		defer shardServer.Close()

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		// Use an invalid path (directory that doesn't exist)
		destPath := "/nonexistent/directory/that/does/not/exist/file.txt"

		err := DownloadFile(context.Background(), cfg, TestFileID, destPath)
		if err == nil {
			t.Fatal("expected error for invalid destination path, got nil")
		}
		if !strings.Contains(err.Error(), "failed to create destination file") {
			t.Errorf("expected error to contain 'failed to create destination file', got %v", err)
		}
	})

	t.Run("error - get bucket file info fails", func(t *testing.T) {
		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		tmpDir := t.TempDir()
		destPath := filepath.Join(tmpDir, "downloaded-file")

		err := DownloadFile(context.Background(), cfg, TestFileID, destPath)
		if err == nil {
			t.Fatal("expected error when get bucket file info fails, got nil")
		}
		if !strings.Contains(err.Error(), "failed to get bucket file info") {
			t.Errorf("expected error to contain 'failed to get bucket file info', got %v", err)
		}
	})

	t.Run("error - generate file key fails with invalid index", func(t *testing.T) {
		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: "invalid-hex-zzz",
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: "http://test"},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		tmpDir := t.TempDir()
		destPath := filepath.Join(tmpDir, "downloaded-file")

		err := DownloadFile(context.Background(), cfg, TestFileID, destPath)
		if err == nil {
			t.Fatal("expected error when generate file key fails, got nil")
		}
		if !strings.Contains(err.Error(), "failed to generate file key") {
			t.Errorf("expected error to contain 'failed to generate file key', got %v", err)
		}
	})
}

func TestGetStartByteAndEndByte(t *testing.T) {
	testCases := []struct {
		name          string
		rangeHeader   string
		expectedStart int
		expectedEnd   int
		expectError   bool
		errorContains string
	}{
		{
			name:          "valid range with end byte",
			rangeHeader:   "bytes=100-199",
			expectedStart: 100,
			expectedEnd:   199,
			expectError:   false,
		},
		{
			name:          "valid range without end byte",
			rangeHeader:   "bytes=100-",
			expectedStart: 100,
			expectedEnd:   -1,
			expectError:   false,
		},
		{
			name:          "range starting at zero",
			rangeHeader:   "bytes=0-99",
			expectedStart: 0,
			expectedEnd:   99,
			expectError:   false,
		},
		{
			name:          "invalid - missing bytes= prefix",
			rangeHeader:   "100-199",
			expectError:   true,
			errorContains: "invalid Range header format",
		},
		{
			name:          "invalid - wrong format",
			rangeHeader:   "bytes=100",
			expectError:   true,
			errorContains: "invalid Range header format",
		},
		{
			name:          "invalid - multiple ranges",
			rangeHeader:   "bytes=0-99,200-299",
			expectError:   true,
			errorContains: "invalid Range header format",
		},
		{
			name:          "invalid - negative start",
			rangeHeader:   "bytes=-200",
			expectError:   true,
			errorContains: "invalid start byte",
		},
		{
			name:          "invalid - non-numeric start",
			rangeHeader:   "bytes=abc-199",
			expectError:   true,
			errorContains: "invalid start byte",
		},
		{
			name:          "invalid - non-numeric end",
			rangeHeader:   "bytes=100-abc",
			expectError:   true,
			errorContains: "invalid end byte",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start, end, err := getStartByteAndEndByte(tc.rangeHeader)

			if tc.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if tc.errorContains != "" && !strings.Contains(err.Error(), tc.errorContains) {
					t.Errorf("expected error to contain %q, got %q", tc.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if start != tc.expectedStart {
					t.Errorf("expected start %d, got %d", tc.expectedStart, start)
				}
				if end != tc.expectedEnd {
					t.Errorf("expected end %d, got %d", tc.expectedEnd, end)
				}
			}
		})
	}
}

func TestAdjustIV(t *testing.T) {
	t.Run("increment by one block", func(t *testing.T) {
		iv := make([]byte, 16)
		originalIV := make([]byte, 16)
		copy(originalIV, iv)

		adjustIV(iv, 1)

		// IV should be incremented by 1
		expected := make([]byte, 16)
		copy(expected, originalIV)
		expected[15]++

		if !bytes.Equal(iv, expected) {
			t.Errorf("expected IV to be incremented, got %v, expected %v", iv, expected)
		}
	})

	t.Run("increment by multiple blocks", func(t *testing.T) {
		iv := make([]byte, 16)
		iv[15] = 255 // Set last byte to max to test carry-over

		adjustIV(iv, 1)

		if iv[15] != 0 {
			t.Errorf("expected last byte to wrap to 0, got %d", iv[15])
		}
		if iv[14] != 1 {
			t.Errorf("expected second-to-last byte to increment, got %d", iv[14])
		}
	})

	t.Run("increment by zero blocks", func(t *testing.T) {
		iv := make([]byte, 16)
		originalIV := make([]byte, 16)
		copy(originalIV, iv)

		adjustIV(iv, 0)

		if !bytes.Equal(iv, originalIV) {
			t.Error("expected IV to remain unchanged when incrementing by 0 blocks")
		}
	})

	t.Run("increment by large number", func(t *testing.T) {
		iv := make([]byte, 16)
		originalIV := make([]byte, 16)
		copy(originalIV, iv)

		adjustIV(iv, 100)

		// IV should be incremented by 100
		expected := make([]byte, 16)
		copy(expected, originalIV)
		expected[15] += 100

		if !bytes.Equal(iv, expected) {
			t.Errorf("expected IV to be incremented by 100, got %v", iv)
		}
	})
}

func TestDownloadFileStream(t *testing.T) {
	t.Run("successful stream download without range", func(t *testing.T) {
		testData := []byte("test file content for streaming")
		plainIndex := TestIndex
		key, iv, err := GenerateFileKey(TestMnemonic, TestBucket6, plainIndex)
		if err != nil {
			t.Fatalf("failed to generate key: %v", err)
		}

		block, _ := aes.NewCipher(key)
		stream := cipher.NewCTR(block, iv)
		encryptedData := make([]byte, len(testData))
		stream.XORKeyStream(encryptedData, testData)

		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write(encryptedData)
		}))
		defer shardServer.Close()

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		readCloser, err := DownloadFileStream(context.Background(), cfg, TestFileID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer readCloser.Close()

		downloadedData, err := io.ReadAll(readCloser)
		if err != nil {
			t.Fatalf("failed to read stream: %v", err)
		}

		if !bytes.Equal(downloadedData, testData) {
			t.Errorf("expected content %q, got %q", string(testData), string(downloadedData))
		}
	})

	t.Run("successful stream download with range", func(t *testing.T) {
		testData := make([]byte, 100)
		rand.Read(testData)
		plainIndex := TestIndex
		key, iv, err := GenerateFileKey(TestMnemonic, TestBucket6, plainIndex)
		if err != nil {
			t.Fatalf("failed to generate key: %v", err)
		}

		block, _ := aes.NewCipher(key)
		encryptStream := cipher.NewCTR(block, iv)
		encryptedData := make([]byte, len(testData))
		encryptStream.XORKeyStream(encryptedData, testData)

		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rangeHeader := r.Header.Get("Range")
			if rangeHeader == "" {
				w.WriteHeader(http.StatusOK)
				w.Write(encryptedData)
				return
			}
			w.Header().Set("Content-Range", "bytes 10-49/100")
			w.WriteHeader(http.StatusPartialContent)
			w.Write(encryptedData[10:50])
		}))
		defer shardServer.Close()

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		readCloser, err := DownloadFileStream(context.Background(), cfg, TestFileID, "bytes=16-47")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer readCloser.Close()

		downloadedData, err := io.ReadAll(readCloser)
		if err != nil {
			t.Fatalf("failed to read stream: %v", err)
		}

		if len(downloadedData) == 0 {
			t.Error("expected non-empty downloaded data")
		}
	})

	t.Run("error - invalid range format", func(t *testing.T) {
		plainIndex := TestIndex

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: "http://test"},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		_, err := DownloadFileStream(context.Background(), cfg, TestFileID, "invalid-range")
		if err == nil {
			t.Fatal("expected error for invalid range, got nil")
		}
		if !strings.Contains(err.Error(), "invalid range") {
			t.Errorf("expected error to contain 'invalid range', got %v", err)
		}
	})

	t.Run("error - no shards", func(t *testing.T) {
		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index:  TestIndex,
				Shards: []ShardInfo{},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		_, err := DownloadFileStream(context.Background(), cfg, TestFileID)
		if err == nil {
			t.Fatal("expected error for no shards, got nil")
		}
		if !strings.Contains(err.Error(), "no shards found") {
			t.Errorf("expected error to contain 'no shards found', got %v", err)
		}
	})

	t.Run("error - get bucket file info fails", func(t *testing.T) {
		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal error"))
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		_, err := DownloadFileStream(context.Background(), cfg, TestFileID)
		if err == nil {
			t.Fatal("expected error when get bucket file info fails, got nil")
		}
		if !strings.Contains(err.Error(), "failed to get bucket file info") {
			t.Errorf("expected error to contain 'failed to get bucket file info', got %v", err)
		}
	})

	t.Run("error - generate file key fails with invalid index", func(t *testing.T) {
		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: "invalid-hex-index-zzz",
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: "http://test"},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		_, err := DownloadFileStream(context.Background(), cfg, TestFileID)
		if err == nil {
			t.Fatal("expected error when generate file key fails, got nil")
		}
		if !strings.Contains(err.Error(), "failed to generate file key") {
			t.Errorf("expected error to contain 'failed to generate file key', got %v", err)
		}
	})

	t.Run("error - shard download HTTP client fails", func(t *testing.T) {
		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: TestIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: "http://invalid-host-that-does-not-exist-12345.local"},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		_, err := DownloadFileStream(context.Background(), cfg, TestFileID)
		if err == nil {
			t.Fatal("expected error when shard download fails, got nil")
		}
		if !strings.Contains(err.Error(), "failed to execute download stream request") {
			t.Errorf("expected error to contain 'failed to execute download stream request', got %v", err)
		}
	})

	t.Run("error - shard download returns non-2xx status", func(t *testing.T) {
		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("shard not found"))
		}))
		defer shardServer.Close()

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: TestIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		_, err := DownloadFileStream(context.Background(), cfg, TestFileID)
		if err == nil {
			t.Fatal("expected error when shard download returns 404, got nil")
		}
		if !strings.Contains(err.Error(), "shard download failed") {
			t.Errorf("expected error to contain 'shard download failed', got %v", err)
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("expected error to contain '404', got %v", err)
		}
	})

	t.Run("error - decrypt reader fails with wrong key size", func(t *testing.T) {
		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("encrypted data"))
		}))
		defer shardServer.Close()

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: "bad-index-short",
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		_, err := DownloadFileStream(context.Background(), cfg, TestFileID)
		if err == nil {
			t.Fatal("expected error when decrypt reader fails, got nil")
		}
	})

	t.Run("successful stream with unaligned range (triggers recursive adjustment)", func(t *testing.T) {
		testData := make([]byte, 128)
		rand.Read(testData)
		plainIndex := TestIndex
		key, iv, err := GenerateFileKey(TestMnemonic, TestBucket6, plainIndex)
		if err != nil {
			t.Fatalf("failed to generate key: %v", err)
		}

		block, _ := aes.NewCipher(key)
		encryptStream := cipher.NewCTR(block, iv)
		encryptedData := make([]byte, len(testData))
		encryptStream.XORKeyStream(encryptedData, testData)

		callCount := 0
		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount++
			rangeHeader := r.Header.Get("Range")
			if rangeHeader == "" {
				w.WriteHeader(http.StatusOK)
				w.Write(encryptedData)
				return
			}

			// Handle adjusted aligned range request
			if strings.Contains(rangeHeader, "bytes=16-63") {
				w.Header().Set("Content-Range", "bytes 16-63/128")
				w.WriteHeader(http.StatusPartialContent)
				w.Write(encryptedData[16:64])
				return
			}

			w.WriteHeader(http.StatusBadRequest)
		}))
		defer shardServer.Close()

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		// Request unaligned range: 20-63 (20 % 16 = 4, not aligned)
		// Should trigger recursive call with adjusted range 16-63
		readCloser, err := DownloadFileStream(context.Background(), cfg, TestFileID, "bytes=20-63")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer readCloser.Close()

		downloadedData, err := io.ReadAll(readCloser)
		if err != nil {
			t.Fatalf("failed to read stream: %v", err)
		}

		if callCount < 2 {
			t.Logf("warning: expected recursive call, got %d calls", callCount)
		}

		// Length should be 44 bytes (20-63 inclusive = 44 bytes)
		if len(downloadedData) != 44 {
			t.Errorf("expected 44 bytes, got %d", len(downloadedData))
		}
	})

	t.Run("successful stream with unaligned open-ended range", func(t *testing.T) {
		testData := make([]byte, 128)
		rand.Read(testData)
		plainIndex := TestIndex
		key, iv, err := GenerateFileKey(TestMnemonic, TestBucket6, plainIndex)
		if err != nil {
			t.Fatalf("failed to generate key: %v", err)
		}

		block, _ := aes.NewCipher(key)
		encryptStream := cipher.NewCTR(block, iv)
		encryptedData := make([]byte, len(testData))
		encryptStream.XORKeyStream(encryptedData, testData)

		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rangeHeader := r.Header.Get("Range")
			if rangeHeader == "" {
				w.WriteHeader(http.StatusOK)
				w.Write(encryptedData)
				return
			}

			// Handle adjusted aligned range request for open-ended
			if strings.Contains(rangeHeader, "bytes=48-") {
				w.Header().Set("Content-Range", "bytes 48-127/128")
				w.WriteHeader(http.StatusPartialContent)
				w.Write(encryptedData[48:])
				return
			}

			w.WriteHeader(http.StatusBadRequest)
		}))
		defer shardServer.Close()

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		// Request unaligned open-ended range: bytes=50- (50 % 16 = 2, not aligned)
		// Should trigger recursive call with adjusted range 48-
		readCloser, err := DownloadFileStream(context.Background(), cfg, TestFileID, "bytes=50-")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer readCloser.Close()

		downloadedData, err := io.ReadAll(readCloser)
		if err != nil {
			t.Fatalf("failed to read stream: %v", err)
		}

		// Should be 78 bytes (50 to 127 inclusive)
		expectedLen := 128 - 50
		if len(downloadedData) != expectedLen {
			t.Errorf("expected %d bytes, got %d", expectedLen, len(downloadedData))
		}
	})

	t.Run("error - recursive download fails during offset discard", func(t *testing.T) {
		testData := make([]byte, 128)
		rand.Read(testData)
		plainIndex := TestIndex
		key, iv, err := GenerateFileKey(TestMnemonic, TestBucket6, plainIndex)
		if err != nil {
			t.Fatalf("failed to generate key: %v", err)
		}

		block, _ := aes.NewCipher(key)
		encryptStream := cipher.NewCTR(block, iv)
		encryptedData := make([]byte, len(testData))
		encryptStream.XORKeyStream(encryptedData, testData)

		callCount := 0
		shardServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount++
			rangeHeader := r.Header.Get("Range")

			// First call (adjusted aligned request) - return less data than expected
			// This will cause io.CopyN to fail during offset discard
			if strings.Contains(rangeHeader, "bytes=16-") && callCount == 1 {
				w.Header().Set("Content-Range", "bytes 16-20/128")
				w.WriteHeader(http.StatusPartialContent)
				// Return only 2 bytes when trying to discard 5 bytes (21-16=5)
				w.Write(encryptedData[16:18])
				return
			}

			w.WriteHeader(http.StatusBadRequest)
		}))
		defer shardServer.Close()

		infoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := BucketFileInfo{
				Index: plainIndex,
				Shards: []ShardInfo{
					{Index: 0, Hash: "hash1", URL: shardServer.URL},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(info)
		}))
		defer infoServer.Close()

		cfg := &config.Config{
			BasicAuthHeader: TestBasicAuth,
			HTTPClient:      &http.Client{},
			Endpoints:       endpoints.NewConfig(infoServer.URL),
			Bucket:          TestBucket6,
			Mnemonic:        TestMnemonic,
		}

		// Request unaligned range that will trigger recursive call and fail during discard
		_, err = DownloadFileStream(context.Background(), cfg, TestFileID, "bytes=21-")
		if err == nil {
			t.Fatal("expected error during offset discard, got nil")
		}
		if !strings.Contains(err.Error(), "failed to discard offset bytes") {
			t.Errorf("expected error to contain 'failed to discard offset bytes', got %v", err)
		}
	})
}
